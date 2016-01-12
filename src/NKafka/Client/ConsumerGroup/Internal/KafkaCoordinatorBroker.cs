using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.Internal;
using NKafka.Connection;
using NKafka.Protocol;
using NKafka.Protocol.API.JoinGroup;
using NKafka.Protocol.API.SyncGroup;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaCoordinatorGroup> _groups;
        [NotNull] private readonly Dictionary<string, int> _joinGroupRequests;
        [NotNull] private readonly Dictionary<string, int> _topicsMetadataRequests;

        private readonly TimeSpan _coordinatorClientTimeout;

        public KafkaCoordinatorBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _groups = new ConcurrentDictionary<string, KafkaCoordinatorGroup>();            
            _joinGroupRequests = new Dictionary<string, int>();
            _topicsMetadataRequests = new Dictionary<string, int>();
            _coordinatorClientTimeout = consumePeriod + TimeSpan.FromSeconds(1) + consumePeriod;
        }

        public void RemoveGroup([NotNull] string groupName)
        {
            KafkaCoordinatorGroup group;
            _groups.TryRemove(groupName, out group);
        }

        public void AddGroup([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup groupCoordinator)
        {
            _groups[groupName] = groupCoordinator;
        }

        public void Process()
        {
            foreach (var groupPair in _groups)
            {
                var groupName = groupPair.Key;
                var group = groupPair.Value;
                ProcessGroup(groupName, group);
            }
        }

        private void ProcessGroup([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup group)
        {
            if (group.Status == KafkaCoordinatorGroupStatus.RearrageRequired)
            {
                return;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.NotInitialized)
            {
                var topics = group.Topics;
                if (topics.Count == 0) return;

                foreach (var topic in topics)
                {
                    if (topic.Status != KafkaClientTopicStatus.Ready) return;
                }

                foreach (var topic in topics)
                {
                    var partitions = topic.Partitions;
                    var partitionIds = new List<int>(partitions.Count);
                    foreach (var partition in partitions)
                    {
                        partitionIds.Add(partition.PartitionId);
                    }
                    group.TopicPartitions[topic.TopicName] = partitionIds;
                }

                var joinRequest = CreateJoinGroupRequest(groupName, group);
                var joinRequestId = SendRequest(joinRequest, _coordinatorClientTimeout + group.Settings.GroupInitiationServerWaitTime);

                if (joinRequestId == null) return;
                _joinGroupRequests[groupName] = joinRequestId.Value;
                group.Status = KafkaCoordinatorGroupStatus.JoinGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinGroupRequested)
            {
                int joinGroupRequestId;
                if (!_joinGroupRequests.TryGetValue(groupName, out joinGroupRequestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var joinGroupResponse = _broker.Receive<KafkaJoinGroupResponse>(joinGroupRequestId);
                if (!joinGroupResponse.HasData) return;

                _joinGroupRequests.Remove(groupName);
                if (!TryProcessJoinGroupResponse(group, joinGroupResponse.Data)) return;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsRequired)
            {
                var topicMetadataRequest = new KafkaTopicMetadataRequest(group.AdditionalTopicNames);
                var topicMetadataRequestId = SendRequest(topicMetadataRequest, _coordinatorClientTimeout);
                if (topicMetadataRequestId == null) return;

                _topicsMetadataRequests[groupName] = topicMetadataRequestId.Value;
                group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested)
            {
                int topicMetadataRequestId;
                if (!_topicsMetadataRequests.TryGetValue(groupName, out topicMetadataRequestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsRequired;
                    return;
                }

                var topicMetadataResponse = _broker.Receive<KafkaTopicMetadataResponse>(topicMetadataRequestId);
                if (!topicMetadataResponse.HasData) return;

                _topicsMetadataRequests.Remove(groupName);
                if (!TryProcessTopicMetadata(group, topicMetadataResponse.Data)) return;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsLeader)
            {
                var assignment = AssignTopics(group);
                if (assignment == null)
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var syncRequest = CreateSyncGroupRequest(groupName, group, assignment);
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsMember)
            {
                var syncRequest = CreateSyncGroupRequest(groupName, group);
            }
        }

        private int? SendRequest<TRequest>(TRequest request, TimeSpan timeout)
            where TRequest: class, IKafkaRequest
        {
            if (request == null) return null;

            var requestResult = _broker.Send(request, timeout);
            if (!requestResult.HasData) return null; //todo (E009)

            var requestId = requestResult.Data;

            return requestId;
        }

        #region JoinGroup

        private bool TryProcessJoinGroupResponse([NotNull] KafkaCoordinatorGroup group, KafkaJoinGroupResponse response)
        {
            if (response == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }

            if (response.ErrorCode == KafkaResponseErrorCode.NotCoordinatorForGroup)
            {
                group.Status = KafkaCoordinatorGroupStatus.RearrageRequired;
                return false;
            }

            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                //todo (E009)
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }

            group.MemberId = response.MemberId;
            group.GroupGenerationId = response.GroupGenerationId;
            group.GroupProtocolName = response.GroupProtocolName;

            if (response.MemberId != response.GroupLeaderId)
            {
                group.IsLeader = false;
                group.Status = KafkaCoordinatorGroupStatus.JoinedAsMember;
                return true;
            }
            
            group.IsLeader = true;                
            var protocolName = response.GroupProtocolName;                
            var responseMembers = response.Members;                

            var topicMembers = new Dictionary<string, List<KafkaCoordinatorGroupMember>>(group.Topics.Count);
            foreach (var topic in group.Topics)
            {
                topicMembers[topic.TopicName] = new List<KafkaCoordinatorGroupMember>();
            }
            var additionalTopics = new List<string>();

            var groupMembers = new List<KafkaCoordinatorGroupMember>();
            if (responseMembers != null)
            {
                foreach (var responseMember in response.Members)
                {
                    if (responseMember == null) continue;

                    var isLeader = responseMember.MemberId == response.GroupLeaderId;
                    var member = new KafkaCoordinatorGroupMember(responseMember.MemberId, isLeader,
                        responseMember.ProtocolVersion,
                        responseMember.AssignmentStrategies ?? new string[0], 
                        responseMember.CustomData);

                    foreach (var topicName in responseMember.TopicNames ?? new string[0])
                    {
                        if (string.IsNullOrEmpty(topicName)) continue;

                        List<KafkaCoordinatorGroupMember> memberList;
                        if (!topicMembers.TryGetValue(topicName, out memberList))
                        {
                            memberList = new List<KafkaCoordinatorGroupMember>();
                            topicMembers[topicName] = memberList;
                            additionalTopics.Add(topicName);
                        }
                        memberList.Add(member);
                    }

                    groupMembers.Add(member);
                }
            }

            group.GroupMembers = groupMembers;
            group.TopicMembers = topicMembers;
            group.AdditionalTopicNames = additionalTopics;

            if (additionalTopics.Count == 0)
            {
                group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;
                return true;
            }

            group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsRequired;            

            return true;
        }

        private KafkaJoinGroupRequest CreateJoinGroupRequest([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup group)
        {
            var topics = group.Topics;
            if (topics.Count == 0) return null;

            var topicNames = new List<string>(topics.Count);
            foreach (var topic in topics)
            {
                topicNames.Add(topic.TopicName);
            }

            var settingsProtocols = group.Settings.Protocols;
            if (settingsProtocols == null || settingsProtocols.Count == 0) return null;

            var protocols = new List<KafkaJoinGroupRequestProtocol>(settingsProtocols.Count);
            foreach (var settingsProtocol in settingsProtocols)
            {
                var protocolName = settingsProtocol?.ProtocolName;
                if (string.IsNullOrEmpty(protocolName)) continue;                

                var settingsAssignmentStrategies = settingsProtocol.AssignmentStrategies;
                if (settingsAssignmentStrategies == null || settingsAssignmentStrategies.Count == 0)
                {
                    continue;
                }

                var assignmentStrategies = new List<string>(settingsAssignmentStrategies.Count);
                foreach (var settingsAssignmentStrategy in settingsAssignmentStrategies)
                {                    
                    var strategyName = settingsAssignmentStrategy?.StrategyName;
                    if (string.IsNullOrEmpty(strategyName)) continue;
                    assignmentStrategies.Add(settingsAssignmentStrategy.StrategyName);
                }
                if (assignmentStrategies.Count == 0)
                {
                    continue;
                }

                var protocolVersion = settingsProtocol.ProtocolVersion;
                var customData = settingsProtocol.CustomData;

                var protocol = new KafkaJoinGroupRequestProtocol(protocolName, protocolVersion, topicNames, assignmentStrategies, customData);
                protocols.Add(protocol);
            }

            if (protocols.Count == 0) return null;
            
            var request = new KafkaJoinGroupRequest(groupName, group.MemberId, group.Settings.GroupSessionTimeout, protocols);
            return request;
        }

        #endregion JoinGroup

        #region Additional topics

        private bool TryProcessTopicMetadata([NotNull] KafkaCoordinatorGroup group, KafkaTopicMetadataResponse response)
        {
            if (response == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsRequired;
                return false;
            }

            var responseTopics = response.Topics ?? new KafkaTopicMetadataResponseTopic[0];            

            foreach (var responseTopic in responseTopics)
            {
                var topicName = responseTopic?.TopicName;
                if (string.IsNullOrEmpty(topicName)) continue;

                var responsePartitons = responseTopic.Partitions;
                if (responsePartitons == null) continue;

                var partitions = new List<int>(responsePartitons.Count);
                foreach (var responsePartition in responsePartitons)
                {
                    //todo (E009) handling standard errors (responsePartition.ErrorCode)
                    if (responsePartition?.ErrorCode != KafkaResponseErrorCode.NoError) continue;
                    partitions.Add(responsePartition.PartitionId);
                }
                group.TopicPartitions[topicName] = partitions;
            }
                        
            group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;

            return true;
        }

        #endregion Additional topics

        #region Assignment

        private Dictionary<string, KafkaConsumerAssignment> AssignTopics([NotNull] KafkaCoordinatorGroup group)
        {
            var groupMembers = group.GroupMembers;
            var topicMembers = group.TopicMembers;
            if (topicMembers == null || groupMembers == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return null;
            }

            var settingsProtocols = group.Settings.Protocols;            
            if (settingsProtocols == null || settingsProtocols.Count == 0)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return null;
            }

            var groupProtocols = new Dictionary<short, List<KafkaConsumerAssignmentStrategyInfo>>();
            var groupProtocolVersions = new List<short>();
            foreach (var settingsProtocol in settingsProtocols)
            {
                if (settingsProtocol == null) continue;                
                if (settingsProtocol.ProtocolName == group.GroupProtocolName)
                {
                    var settingsSrategies = settingsProtocol.AssignmentStrategies;
                    if (settingsSrategies == null || settingsSrategies.Count == 0)
                    {
                        continue;
                    }

                    var strategies = new List<KafkaConsumerAssignmentStrategyInfo>(settingsSrategies.Count);
                    foreach (var settingsStrategy in strategies)
                    {
                        if (settingsStrategy?.StrategyName == null) continue;
                        if (settingsStrategy?.Strategy == null) continue;

                        strategies.Add(settingsStrategy);
                    }
                    if (strategies.Count == 0) continue;

                    groupProtocolVersions.Add(settingsProtocol.ProtocolVersion);
                    groupProtocols[settingsProtocol.ProtocolVersion] = strategies;
                }
            }
            if (groupProtocols.Count == 0)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return null;
            }
            groupProtocolVersions.Sort();
            groupProtocolVersions.Reverse();

            // поиск наменьшей из требуемых версий и множества допустимых стратегий
            short? minProtocolVersion = null;
            HashSet<string> availableStrategies = null;
            foreach (var groupMember in groupMembers)
            {                                
                if (minProtocolVersion == null || minProtocolVersion.Value > groupMember.ProtocolVersion)
                {
                    minProtocolVersion = groupMember.ProtocolVersion;
                }

                var memberStrategies = groupMember.AvailableAssignmentStrategies ?? new string[0];
                if (availableStrategies == null)
                {
                    availableStrategies = new HashSet<string>(memberStrategies);
                }
                else
                {
                    availableStrategies.IntersectWith(memberStrategies);
                }
            }
            if (minProtocolVersion == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return null;
            }
            group.GroupProtocolVersion = minProtocolVersion.Value;       

            // подбираем протокол с наиболее релевантной версией
            short? topicProtocolVersion = null;
            foreach (var groupProtocolVersion in groupProtocolVersions)
            {
                if (groupProtocolVersion <= minProtocolVersion)
                {
                    topicProtocolVersion = groupProtocolVersion;
                    break;
                }
            }
            if (topicProtocolVersion == null)
            {
                topicProtocolVersion = groupProtocolVersions[0];
            }
            var protocolStrategies = groupProtocols[topicProtocolVersion.Value];

            // в найденном протоколе подбираем наиболее релевантную стратегию
            KafkaConsumerAssignmentStrategyInfo topicStrategy = null;
            foreach (var strategy in protocolStrategies)
            {
                if (availableStrategies?.Contains(strategy.StrategyName) == true)
                {
                    topicStrategy = strategy;
                    break;
                }
            }
            if (topicStrategy == null)
            {
                topicStrategy = protocolStrategies[0];
            }

            // Поочерёдное назначение партиций на каждый топик
            var topicAssignments = new Dictionary<string, KafkaConsumerAssignment>(topicMembers.Count);
            foreach (var topicMember in topicMembers)
            {
                var topicName = topicMember.Key;
                var members = topicMember.Value;
                if (members == null || members.Count == 0) continue;

                IReadOnlyList<int> partitionIds;
                if (!group.TopicPartitions.TryGetValue(topicName, out partitionIds))
                {
                    continue;
                }

                var assignmentMembers = new List<KafkaConsumerAssignmentRequestMember>(members.Count);

                foreach (var member in members)
                {
                    assignmentMembers.Add(new KafkaConsumerAssignmentRequestMember(member.MemberId, member.IsLeader,
                        member.CustomData));
                }

                // процедура назначения в соответствии с выбранной стратегией                
                var assignmentRequest = new KafkaConsumerAssignmentRequest(topicName, partitionIds, assignmentMembers);
                KafkaConsumerAssignment assignment;
                try
                {
                    assignment = topicStrategy.Strategy.Assign(assignmentRequest);
                }
                catch (Exception)
                {
                    assignment = null;
                }

                if (assignment == null)
                {
                    continue;
                }
            }

            return topicAssignments;
        }

        #endregion Assignment

        #region SyncGroup

        private KafkaSyncGroupRequest CreateSyncGroupRequest([NotNull] string groupName, [NotNull] KafkaCoordinatorGroup group,
            [CanBeNull] IReadOnlyDictionary<string, KafkaConsumerAssignment> assignment = null)
        {
            var groupMembers = group.GroupMembers;

            if (assignment == null || groupMembers == null)
            {
                return new KafkaSyncGroupRequest(groupName, group.GroupGenerationId, group.MemberId, null);
            }

            var requestMembers = new List<KafkaSyncGroupRequestMember>(groupMembers.Count);

            foreach (var groupMember in groupMembers)
            {
                //todo
                var requestMember = new KafkaSyncGroupRequestMember(groupMember.MemberId, group.GroupProtocolVersion, null, groupMember.CustomData);
                requestMembers.Add(requestMember);
            }

            return new KafkaSyncGroupRequest(groupName, group.GroupGenerationId, group.MemberId, requestMembers);
        }

        #endregion SyncGroup
    }
}