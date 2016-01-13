﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.Internal;
using NKafka.Connection;
using NKafka.Protocol;
using NKafka.Protocol.API.Heartbeat;
using NKafka.Protocol.API.JoinGroup;
using NKafka.Protocol.API.OffsetFetch;
using NKafka.Protocol.API.SyncGroup;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaCoordinatorGroup> _groups;
        [NotNull] private readonly Dictionary<string, int> _joinGroupRequests;
        [NotNull] private readonly Dictionary<string, int> _additionalTopicsRequests;
        [NotNull] private readonly Dictionary<string, int> _syncGroupRequests;
        [NotNull] private readonly Dictionary<string, int> _heartbeatRequests;
        [NotNull] private readonly Dictionary<string, int> _offsetFetchRequests;

        private readonly TimeSpan _coordinatorClientTimeout;

        public KafkaCoordinatorBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _groups = new ConcurrentDictionary<string, KafkaCoordinatorGroup>();            
            _joinGroupRequests = new Dictionary<string, int>();
            _syncGroupRequests = new Dictionary<string, int>();
            _additionalTopicsRequests = new Dictionary<string, int>();
            _heartbeatRequests = new Dictionary<string, int>();
            _offsetFetchRequests = new Dictionary<string, int>();
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
                ProcessGroup(groupPair.Value);
            }
        }

        private void ProcessGroup([NotNull] KafkaCoordinatorGroup group)
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

                var joinRequest = CreateJoinGroupRequest(group);
                var requestId = SendRequest(joinRequest, _coordinatorClientTimeout + group.Settings.GroupInitiationServerWaitTime);

                if (requestId == null) return;
                _joinGroupRequests[group.GroupName] = requestId.Value;
                group.Status = KafkaCoordinatorGroupStatus.JoinGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinGroupRequested)
            {
                int requestId;
                if (!_joinGroupRequests.TryGetValue(group.GroupName, out requestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var joinGroupResponse = _broker.Receive<KafkaJoinGroupResponse>(requestId);
                if (!joinGroupResponse.HasData) return;

                _joinGroupRequests.Remove(group.GroupName);
                if (!TryProcessJoinGroupResponse(group, joinGroupResponse.Data))
                {
                    return;
                }

                if (group.MemberIsLeader)
                {
                    if (group.AdditionalTopicNames == null || group.AdditionalTopicNames.Count == 0)
                    {
                        group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;
                    }
                    else
                    {
                        group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsRequired;
                    }
                }
                else
                {
                    group.Status = KafkaCoordinatorGroupStatus.JoinedAsMember;
                }
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsRequired)
            {
                var topicMetadataRequest = new KafkaTopicMetadataRequest(group.AdditionalTopicNames);
                var requestId = SendRequest(topicMetadataRequest, _coordinatorClientTimeout);
                if (requestId == null) return;

                _additionalTopicsRequests[group.GroupName] = requestId.Value;
                group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested)
            {
                int requestId;
                if (!_additionalTopicsRequests.TryGetValue(group.GroupName, out requestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsRequired;
                    return;
                }

                var topicMetadataResponse = _broker.Receive<KafkaTopicMetadataResponse>(requestId);
                if (!topicMetadataResponse.HasData) return;

                _additionalTopicsRequests.Remove(group.GroupName);
                if (!TryProcessAdditionalTopics(group, topicMetadataResponse.Data))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsLeader)
            {
                if (!TryAssignTopics(group))                
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var syncRequest = CreateSyncGroupRequest(group);
                var requestId = SendRequest(syncRequest, _coordinatorClientTimeout + group.Settings.GroupInitiationServerWaitTime);
                if (requestId == null) return;

                _syncGroupRequests[group.GroupName] = requestId.Value;
                group.Status = KafkaCoordinatorGroupStatus.SyncGroupRequested;
            }          

            if (group.Status == KafkaCoordinatorGroupStatus.SyncGroupRequested)
            {
                int requestId;
                if (!_syncGroupRequests.TryGetValue(group.GroupName, out requestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var syncGroupResponse = _broker.Receive<KafkaSyncGroupResponse>(requestId);
                if (!syncGroupResponse.HasData) return;

                _syncGroupRequests.Remove(group.GroupName);
                if (!TryProcessSyncGroupResponse(group, syncGroupResponse.Data))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.FirstHeatbeatRequired;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeatbeatRequired)
            {
                var heartbeatRequest = CreateHeartbeatRequest(group);
                var requestId = SendRequest(heartbeatRequest, _coordinatorClientTimeout + group.Settings.HeartbeatServerWaitTime);
                if (requestId == null) return;

                _heartbeatRequests[group.GroupName] = requestId.Value;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeatbeatRequested)
            {
                int requestId;
                if (!_heartbeatRequests.TryGetValue(group.GroupName, out requestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var heartbeatResponse = _broker.Receive<KafkaHeartbeatResponse>(requestId);
                if (!heartbeatResponse.HasData) return;

                _heartbeatRequests.Remove(group.GroupName);
                if (!TryProcessHeartbeatResponse(group, heartbeatResponse.Data))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequired;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequired)
            {
                var offsetFetchRequest = CreateOffsetFetchRequest(group);
                var requestId = SendRequest(offsetFetchRequest, _coordinatorClientTimeout + group.Settings.OffsetFetchServerWaitTime);
                if (requestId == null) return;

                _offsetFetchRequests[group.GroupName] = requestId.Value;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequested)
            {
                int requestId;
                if (!_offsetFetchRequests.TryGetValue(group.GroupName, out requestId))
                {
                    group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                    return;
                }

                var offstFetchResponse = _broker.Receive<KafkaOffsetFetchResponse>(requestId);
                if (!offstFetchResponse.HasData) return;

                _offsetFetchRequests.Remove(group.GroupName);
                if (!TryProcessOffsetFetchResponse(group, offstFetchResponse.Data))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.Ready;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.Ready)
            {

            }

            //todo (C004) regular heartbeat
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

        private KafkaJoinGroupRequest CreateJoinGroupRequest([NotNull] KafkaCoordinatorGroup group)
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

            var request = new KafkaJoinGroupRequest(group.GroupName, group.MemberId, group.Settings.GroupSessionTimeout, protocols);
            return request;
        }

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
            
            group.GroupGenerationId = response.GroupGenerationId;
            group.GroupProtocolName = response.GroupProtocolName;
            group.MemberId = response.MemberId;

            if (response.MemberId != response.GroupLeaderId)
            {
                group.MemberIsLeader = false;                
                return true;
            }

            group.MemberIsLeader = true;
                                     
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

            return true;
        }        

        #endregion JoinGroup

        #region Additional topics

        private bool TryProcessAdditionalTopics([NotNull] KafkaCoordinatorGroup group, KafkaTopicMetadataResponse response)
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

            return true;
        }

        #endregion Additional topics

        #region Assignment

        private bool TryAssignTopics([NotNull] KafkaCoordinatorGroup group)
        {
            var groupMembers = group.GroupMembers;
            var topicMembers = group.TopicMembers;
            if (topicMembers == null || groupMembers == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }

            var settingsProtocols = group.Settings.Protocols;            
            if (settingsProtocols == null || settingsProtocols.Count == 0)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }
            
            var groupProtocols = new Dictionary<short, List<KafkaConsumerAssignmentStrategyInfo>>(settingsProtocols.Count);
            var groupProtocolVersions = new List<short>(settingsProtocols.Count);
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
                        if (settingsStrategy.Strategy == null) continue;

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
                return false;
            }
            groupProtocolVersions.Sort();
            groupProtocolVersions.Reverse();

            // aggregate requirements - min supported protocol version and set of supported strategies
            short? minSupportedProtocolVersion = null;
            HashSet<string> supportedStrategies = null;
            var groupMembersDictionary = new Dictionary<string, KafkaCoordinatorGroupMember>(groupMembers.Count);
            foreach (var groupMember in groupMembers)
            {
                groupMembersDictionary[groupMember.MemberId] = groupMember;

                if (minSupportedProtocolVersion == null || minSupportedProtocolVersion.Value > groupMember.ProtocolVersion)
                {
                    minSupportedProtocolVersion = groupMember.ProtocolVersion;
                }

                var memberStrategies = groupMember.SupportedAssignmentStrategies;
                if (supportedStrategies == null)
                {
                    supportedStrategies = new HashSet<string>(memberStrategies);
                }
                else
                {
                    supportedStrategies.IntersectWith(memberStrategies);
                }
            }
            if (minSupportedProtocolVersion == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }            

            // find the most relevant protocol
            short? topicProtocolVersion = null;
            foreach (var groupProtocolVersion in groupProtocolVersions)
            {
                if (groupProtocolVersion <= minSupportedProtocolVersion)
                {
                    topicProtocolVersion = groupProtocolVersion;
                    break;
                }
            }
            if (topicProtocolVersion == null)
            {
                topicProtocolVersion = groupProtocolVersions[0];
            }
            group.GroupProtocolVersion = topicProtocolVersion.Value;
            var protocolStrategies = groupProtocols[topicProtocolVersion.Value];

            // в найденном протоколе подбираем наиболее релевантную стратегию
            KafkaConsumerAssignmentStrategyInfo topicStrategy = null;
            foreach (var strategy in protocolStrategies)
            {
                if (supportedStrategies.Contains(strategy.StrategyName))
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

                var assignmentRequestMembers = new List<KafkaConsumerAssignmentRequestMember>(members.Count);

                foreach (var member in members)
                {
                    assignmentRequestMembers.Add(new KafkaConsumerAssignmentRequestMember(member.MemberId, member.IsLeader,
                        member.CustomData));
                }

                // процедура назначения в соответствии с выбранной стратегией                
                var assignmentRequest = new KafkaConsumerAssignmentRequest(topicName, partitionIds, assignmentRequestMembers);
                KafkaConsumerAssignment assignment;
                try
                {
                    assignment = topicStrategy.Strategy.Assign(assignmentRequest);
                }
                catch (Exception)
                {
                    assignment = null;
                }

                var assignmentMembers = assignment?.Members;
                if (assignmentMembers == null)
                {
                    continue;
                }

                foreach (var assignmentMember in assignmentMembers)
                {
                    if (assignmentMember == null) continue;
                    var assignmentMemberId = assignmentMember.MemberId;
                    var assignmentPartitionIds = assignmentMember.PartitionIds;
                    if (assignmentMemberId == null || assignmentPartitionIds == null || assignmentPartitionIds.Count == 0) continue;

                    KafkaCoordinatorGroupMember groupMember;
                    if (!groupMembersDictionary.TryGetValue(assignmentMemberId, out groupMember)) continue;

                    groupMember.TopicAssignments[topicName] = assignmentPartitionIds;
                }
            }

            return true;
        }

        #endregion Assignment

        #region SyncGroup

        private KafkaSyncGroupRequest CreateSyncGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var groupMembers = group.GroupMembers;

            if (groupMembers == null)
            {
                return new KafkaSyncGroupRequest(group.GroupName, group.GroupGenerationId, group.MemberId, null);
            }

            var requestMembers = new List<KafkaSyncGroupRequestMember>(groupMembers.Count);
            
            foreach (var groupMember in groupMembers)
            {
                var groupMemberTopics = new List<KafkaSyncGroupRequestMemberTopic>(groupMember.TopicAssignments.Count);
                foreach (var topicAssignment in groupMember.TopicAssignments)
                {
                    var topicName = topicAssignment.Key;
                    var topicPartitionIds = topicAssignment.Value;
                    if (string.IsNullOrEmpty(topicName) || topicPartitionIds == null || topicPartitionIds.Count == 0) continue;

                    var gropMemberTopic = new KafkaSyncGroupRequestMemberTopic(topicName, topicPartitionIds);
                    groupMemberTopics.Add(gropMemberTopic);
                }
                var requestMember = new KafkaSyncGroupRequestMember(groupMember.MemberId, group.GroupProtocolVersion, groupMemberTopics, groupMember.CustomData);
                requestMembers.Add(requestMember);
            }

            return new KafkaSyncGroupRequest(group.GroupName, group.GroupGenerationId, group.MemberId, requestMembers);
        }

        private bool TryProcessSyncGroupResponse([NotNull] KafkaCoordinatorGroup group, KafkaSyncGroupResponse response)
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

            group.GroupProtocolVersion = response.ProtocolVersion;

            var memberAssignment = new Dictionary<string, IReadOnlyList<int>>();
            var assignedTopics = response.AssignedTopics ?? new KafkaSyncGroupResponseTopic[0];
            foreach (var assignedTopic in assignedTopics)
            {
                if (assignedTopic == null) continue;
                var topicName = assignedTopic.TopicName;
                var partitionIds = assignedTopic.PartitionIds;
                if (string.IsNullOrEmpty(topicName) || partitionIds == null || partitionIds.Count == 0) continue;

                memberAssignment[topicName] = partitionIds;
            }

            group.MemberAssignment = memberAssignment;            

            return true;
        }

        #endregion SyncGroup

        #region Heartbeat

        private KafkaHeartbeatRequest CreateHeartbeatRequest([NotNull] KafkaCoordinatorGroup group)
        {
            return new KafkaHeartbeatRequest(group.GroupName, group.GroupGenerationId, group.MemberId);
        }

        private bool TryProcessHeartbeatResponse([NotNull] KafkaCoordinatorGroup group, KafkaHeartbeatResponse response)
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
                        
            return true;
        }
        #endregion Heartbeat

        #region OffsetFetch

        private KafkaOffsetFetchRequest CreateOffsetFetchRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var memberAssignment = group.MemberAssignment ?? new Dictionary<string, IReadOnlyList<int>>();

            var topics = new List<KafkaOffsetFetchRequestTopic>(memberAssignment.Count);
            foreach (var memberTopicAssignment in memberAssignment)
            {
                var topicName = memberTopicAssignment.Key;
                var topicPartitionIds = memberTopicAssignment.Value;

                topics.Add(new KafkaOffsetFetchRequestTopic(topicName, topicPartitionIds));
            }

            return new KafkaOffsetFetchRequest(group.GroupName, topics);
        }

        private bool TryProcessOffsetFetchResponse([NotNull] KafkaCoordinatorGroup group, KafkaOffsetFetchResponse response)
        {
            if (response == null)
            {
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }

            var topics = response.Topics;
            if (topics == null || topics.Count == 0)
            {
                return true;
            }

            foreach (var topic in topics)
            {
                if (topic == null) continue;
                var topicName = topic.TopicName;
                var topicPartitions = topic.Partitions;
                if (string.IsNullOrEmpty(topicName) || topicPartitions == null) continue;

                foreach (var partition in topicPartitions)
                {
                    if (partition == null) continue;
                    if (partition.ErrorCode == KafkaResponseErrorCode.NotCoordinatorForGroup)
                    {
                        group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                        return false;
                    }
                    if (partition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        //todo (E009)
                        group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                        return false;
                    }
                }                
            }

            //todo set offsets

            return true;
        }

        #endregion OffsetFetch
    }
}