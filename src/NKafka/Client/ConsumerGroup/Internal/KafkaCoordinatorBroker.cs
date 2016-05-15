using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.ConsumerGroup.Logging;
using NKafka.Client.Internal;
using NKafka.Connection;
using NKafka.Connection.Diagnostics;
using NKafka.Protocol;
using NKafka.Protocol.API.Heartbeat;
using NKafka.Protocol.API.JoinGroup;
using NKafka.Protocol.API.LeaveGroup;
using NKafka.Protocol.API.OffsetCommit;
using NKafka.Protocol.API.OffsetFetch;
using NKafka.Protocol.API.SyncGroup;
using NKafka.Protocol.API.TopicMetadata;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly IKafkaClientBroker _clientBroker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaCoordinatorGroup> _groups;
        [NotNull] private readonly Dictionary<string, int> _joinGroupRequests;
        [NotNull] private readonly Dictionary<string, int> _additionalTopicsRequests;
        [NotNull] private readonly Dictionary<string, int> _syncGroupRequests;
        [NotNull] private readonly Dictionary<string, int> _heartbeatRequests;
        [NotNull] private readonly Dictionary<string, int> _offsetFetchRequests;
        [NotNull] private readonly Dictionary<string, int> _offsetCommitRequests;

        private readonly TimeSpan _coordinatorClientTimeout;

        private const int DefaultGenerationId = -1;
        private const int DefaultProtocolVersion = -1;
        // ReSharper disable once InconsistentNaming
        private readonly string DefaultMemberId = string.Empty;

        public KafkaCoordinatorBroker([NotNull] KafkaBroker broker, [NotNull] IKafkaClientBroker clientBroker, TimeSpan consumePeriod)
        {
            _broker = broker;
            _clientBroker = clientBroker;
            _groups = new ConcurrentDictionary<string, KafkaCoordinatorGroup>();            
            _joinGroupRequests = new Dictionary<string, int>();
            _syncGroupRequests = new Dictionary<string, int>();
            _additionalTopicsRequests = new Dictionary<string, int>();
            _heartbeatRequests = new Dictionary<string, int>();
            _offsetFetchRequests = new Dictionary<string, int>();
            _offsetCommitRequests = new Dictionary<string, int>();

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

        public void Process(CancellationToken cancellation)
        {
            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                if (cancellation.IsCancellationRequested) return;
                ProcessGroup(group, cancellation);
            }
        }

        public void Start()
        {            
        }

        public void Stop()
        {
            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                if (group.Status == KafkaCoordinatorGroupStatus.Ready)
                {
                    var commitRequest = CreateOffsetCommitRequest(group);
                    if (commitRequest != null)
                    {
                        _broker.SendWithoutResponse(commitRequest, group.GroupCoordinatorName);
                    }
                }

                if (group.GroupType == KafkaConsumerGroupType.BalancedConsumers && group.Status >= KafkaCoordinatorGroupStatus.JoinGroupRequested)
                {
                    var leaveGroupRequest = CreateLeaveGroupRequest(group);
                    if (leaveGroupRequest != null)
                    {
                        _broker.SendWithoutResponse(leaveGroupRequest, group.GroupCoordinatorName);
                    }
                }
                                
                group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
                group.Clear();
            }

            _joinGroupRequests.Clear();
            _additionalTopicsRequests.Clear();
            _syncGroupRequests.Clear();
            _heartbeatRequests.Clear();
            _offsetFetchRequests.Clear();
            _offsetCommitRequests.Clear();
        }

        private void ProcessGroup([NotNull] KafkaCoordinatorGroup group, CancellationToken cancellation)
        {
            if (cancellation.IsCancellationRequested) return;

            if (group.Status == KafkaCoordinatorGroupStatus.RearrangeRequired)
            {
                return;
            }
           
            if (group.Status == KafkaCoordinatorGroupStatus.Error)
            {
                if (DateTime.UtcNow - group.ErrorTimestampUtc < group.Settings.ErrorRetryPeriod)
                {
                    return;
                }                
            }

            if (group.Status == KafkaCoordinatorGroupStatus.NotInitialized || 
                group.Status == KafkaCoordinatorGroupStatus.Error ||
                group.Status == KafkaCoordinatorGroupStatus.Rebalance)
            {
                var topics = group.Topics;
                if (topics.Count == 0) return;

                foreach (var topicPair in topics)
                {
                    var topic = topicPair.Value;
                    if (topic == null) continue;

                    if (topic.Status != KafkaClientTopicStatus.Ready) return;
                }

                foreach (var topicPair in topics)
                {
                    var topic = topicPair.Value;
                    if (topic == null) continue;

                    var partitions = topic.Partitions;
                    var partitionIds = new List<int>(partitions.Count);
                    foreach (var partition in partitions)
                    {
                        partitionIds.Add(partition.PartitionId);
                    }
                    group.TopicMetadataPartitionIds[topic.TopicName] = partitionIds;
                }

                if (group.GroupType == KafkaConsumerGroupType.SingleConsumer)
                {
                    group.SetMemberData(DefaultGenerationId, DefaultMemberId, false);
                    group.SetAssignmentData(group.TopicMetadataPartitionIds);
                    group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequired;
                }

                if (group.GroupType == KafkaConsumerGroupType.Observer)
                {
                    group.SetMemberData(DefaultGenerationId, DefaultMemberId, false);
                    group.SetAssignmentData(group.TopicMetadataPartitionIds);
                    group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequired;
                }

                if (group.GroupType == KafkaConsumerGroupType.BalancedConsumers)
                {
                    var joinRequest = CreateJoinGroupRequest(group);
                    if (joinRequest == null) return;

                    if (!TrySendRequest(group, joinRequest, "SendJoinGroupRequest",
                            _joinGroupRequests,_coordinatorClientTimeout + group.Settings.JoinGroupServerTimeout))
                    {
                        return;
                    }

                    group.Status = KafkaCoordinatorGroupStatus.JoinGroupRequested;
                }                
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinGroupRequested)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryHandleResponse<KafkaJoinGroupResponse>(group, "ReceiveJoinGroupResponse",
                        _joinGroupRequests, TryHandleJoinGroupResponse))
                {
                    return;
                }

                if (group.MemberData?.IsLeader == true)
                {
                    var addtionalTopicNames = group.LeaderData?.AdditionalTopicNames;
                    if (addtionalTopicNames == null || addtionalTopicNames.Count == 0)
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
                if (cancellation.IsCancellationRequested) return;

                var addtionalTopicNames = group.LeaderData?.AdditionalTopicNames;
                if (addtionalTopicNames == null || addtionalTopicNames.Count == 0)
                {
                    group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;
                }
                else
                {
                    var topicMetadataRequest = new KafkaTopicMetadataRequest(addtionalTopicNames);
                    if (!TrySendRequest(group, topicMetadataRequest, "SendTopicMetadataRequest(additional topics)",
                        _additionalTopicsRequests, _coordinatorClientTimeout))
                    {
                        return;
                    }

                    group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested;
                }                
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryHandleResponse<KafkaTopicMetadataResponse>(group, "ReceiveSendTopicMetadataResponse(additional topics)",
                        _additionalTopicsRequests, TryHandleAdditionalTopics))
                {
                    return;
                }                

                group.Status = KafkaCoordinatorGroupStatus.JoinedAsLeader;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsLeader)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryAssignTopics(group, "Assignment"))
                {                    
                    return;
                }

                var syncRequest = CreateSyncGroupRequest(group);
                if (!TrySendRequest(group, syncRequest, "SendSyncGroupRequest(leader)",
                        _syncGroupRequests, _coordinatorClientTimeout + group.Settings.SyncGroupServerTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.SyncGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsMember)
            {
                if (cancellation.IsCancellationRequested) return;

                var syncRequest = CreateSyncGroupRequest(group);
                if (!TrySendRequest(group, syncRequest, "SendSyncGroupRequst(member)",
                        _syncGroupRequests, _coordinatorClientTimeout + group.Settings.SyncGroupServerTimeout))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.SyncGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.SyncGroupRequested)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryHandleResponse<KafkaSyncGroupResponse>(group, "ReceiveSyncGroupResponse",
                        _syncGroupRequests, TryHandleSyncGroupResponse))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.FirstHeartbeatRequired;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeartbeatRequired)
            {
                if (cancellation.IsCancellationRequested) return;

                var heartbeatRequest = CreateHeartbeatRequest(group);
                if (!TrySendRequest(group, heartbeatRequest, "SendHeartbeatRequest(first)",
                        _heartbeatRequests, _coordinatorClientTimeout + group.Settings.HeartbeatServerTimeout))
                {
                    return;
                }

                group.HeartbeatTimestampUtc = DateTime.UtcNow;
                group.Status = KafkaCoordinatorGroupStatus.FirstHeatbeatRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeatbeatRequested)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryHandleResponse<KafkaHeartbeatResponse>(group, "ReceiveHeartbeatResponse(first)",
                        _heartbeatRequests, TryHandleHeartbeatResponse))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequired;
            }

            if (group.GroupType == KafkaConsumerGroupType.BalancedConsumers &&
                (group.Status == KafkaCoordinatorGroupStatus.Ready ||
                 group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequired || 
                 group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequested))
            {

                if (cancellation.IsCancellationRequested) return;
                //regular heartbeat


                if (_heartbeatRequests.ContainsKey(group.GroupName))
                {
                    if (!TryHandleResponse<KafkaHeartbeatResponse>(group, "ReceiveHeartbeatResponse",
                            _heartbeatRequests, TryHandleHeartbeatResponse))
                    {
                        return;
                    }
                }

                if (group.HeartbeatTimestampUtc + group.HeartbeatPeriod >= DateTime.UtcNow &&
                    !_heartbeatRequests.ContainsKey(group.GroupName))
                {
                    var heartbeatRequest = CreateHeartbeatRequest(group);
                    if (!TrySendRequest(group, heartbeatRequest, "SendHearbeatRequest",
                            _heartbeatRequests, _coordinatorClientTimeout + group.Settings.HeartbeatServerTimeout))
                    {
                        return;
                    }        

                    group.HeartbeatTimestampUtc = DateTime.UtcNow;                    
                }
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequired)
            {
                if (cancellation.IsCancellationRequested) return;

                var offsetFetchRequest = CreateOffsetFetchRequest(group);
                if (!TrySendRequest(group, offsetFetchRequest, "SendOffsetFetchRequest",
                        _offsetFetchRequests, _coordinatorClientTimeout + group.Settings.OffsetFetchServerTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequested)
            {
                if (cancellation.IsCancellationRequested) return;

                if (!TryHandleResponse<KafkaOffsetFetchResponse>(group, "ReceiveOffsetFetchResponse",
                        _offsetFetchRequests, TryHandleOffsetFetchResponse))
                {
                    return;
                }

                group.CommitTimestampUtc = DateTime.UtcNow;
                group.Status = KafkaCoordinatorGroupStatus.Ready;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.Ready)
            {
                if (cancellation.IsCancellationRequested) return;

                if (group.GroupType == KafkaConsumerGroupType.SingleConsumer ||
                    group.GroupType == KafkaConsumerGroupType.BalancedConsumers)
                {
                    //regular commit

                    if (_offsetCommitRequests.ContainsKey(group.GroupName))
                    {
                        if (!TryHandleResponse<KafkaOffsetCommitResponse>(group, "ReceiveOffsetCommitResponse",
                                _offsetCommitRequests, TryHandleOffsetCommitResponse))
                        {
                            return;
                        }
                    }

                    if (group.CommitTimestampUtc + group.CommitPeriod >= DateTime.UtcNow &&
                        !_offsetCommitRequests.ContainsKey(group.GroupName))
                    {
                        group.CommitTimestampUtc = DateTime.UtcNow;

                        var commitRequest = CreateOffsetCommitRequest(group);
                        if (commitRequest != null)
                        {
                            if (!TrySendRequest(group, commitRequest, "SendOffsetCommitRequest",
                                    _offsetCommitRequests, _coordinatorClientTimeout + group.Settings.OffsetCommitServerTimeout))
                            {
                                return;
                            }
                        }
                    }

                    ResetError(group);
                }

                if (group.GroupType == KafkaConsumerGroupType.Observer)
                {
                    if (_offsetFetchRequests.ContainsKey(group.GroupName))
                    {
                        if (!TryHandleResponse<KafkaOffsetFetchResponse>(group, "ReceiveOffsetFetchResponse",
                                _offsetFetchRequests, TryHandleOffsetFetchResponse))
                        {
                            return;
                        }
                    }

                    if (group.CommitTimestampUtc + group.CommitPeriod >= DateTime.UtcNow &&
                        !_offsetFetchRequests.ContainsKey(group.GroupName))
                    {
                        group.CommitTimestampUtc = DateTime.UtcNow;

                        var offsetFetchRequest = CreateOffsetFetchRequest(group);
                        if (!TrySendRequest(group, offsetFetchRequest, "SendOffsetFetchRequest",
                                _offsetFetchRequests, _coordinatorClientTimeout + group.Settings.OffsetFetchServerTimeout))
                        {
                            return;
                        }
                    }

                    ResetError(group);
                }
            }
        }              

        #region JoinGroup

        [CanBeNull]
        private KafkaJoinGroupRequest CreateJoinGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var topics = group.Topics;
            if (topics.Count == 0) return null;

            var topicNames = new List<string>(topics.Count);
            foreach (var topicPair in topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;
                topicNames.Add(topic.TopicName);
            }

            var settingsProtocols = group.Protocols;            

            var protocols = new List<KafkaJoinGroupRequestProtocol>(settingsProtocols.Count);
            foreach (var settingsProtocol in settingsProtocols)
            {
                var protocolName = settingsProtocol.ProtocolName;                
                var settingsAssignmentStrategies = settingsProtocol.AssignmentStrategies;
                if (settingsAssignmentStrategies == null) continue;
                
                var assignmentStrategies = new List<string>(settingsAssignmentStrategies.Count);
                foreach (var settingsAssignmentStrategy in settingsAssignmentStrategies)
                {                                        
                    if (settingsAssignmentStrategy?.StrategyName == null) continue;
                    assignmentStrategies.Add(settingsAssignmentStrategy.StrategyName);
                }

                var protocolVersion = settingsProtocol.ProtocolVersion;
                var customData = settingsProtocol.CustomData;

                var protocol = new KafkaJoinGroupRequestProtocol(protocolName, protocolVersion, topicNames, assignmentStrategies, customData);
                protocols.Add(protocol);
            }

            if (protocols.Count == 0) return null;

            var sessionTimeout = group.CustomSessionTimeout ?? group.Settings.GroupSessionTimeout;
            var request = new KafkaJoinGroupRequest(group.GroupName, group.MemberData?.MemberId ?? string.Empty, sessionTimeout, protocols);
            return request;
        }

        private bool TryHandleJoinGroupResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaJoinGroupResponse response, string description)
        {            
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaConsumerGroupErrorCode error;
                GroupErrorType errorType;                
                switch (response.ErrorCode)
                {                    
                    case KafkaResponseErrorCode.GroupLoadInProgress:
                        error = KafkaConsumerGroupErrorCode.GroupLoadInProgress;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                        error = KafkaConsumerGroupErrorCode.GroupCoordinatorNotAvailable;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotCoordinatorForGroup:
                        error = KafkaConsumerGroupErrorCode.NotCoordinatorForGroup;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.InconsistentGroupProtocol:
                        error = KafkaConsumerGroupErrorCode.InconsistentGroupProtocol;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.UnknownMemberId:
                        error = KafkaConsumerGroupErrorCode.UnknownMemberId;
                        errorType = GroupErrorType.Error;                        
                        break;                    
                    case KafkaResponseErrorCode.InvalidSessionTimeout:
                        error = KafkaConsumerGroupErrorCode.InvalidSessionTimeout;
                        errorType = GroupErrorType.Warning;
                        var customSessionLifetime = group.CustomSessionTimeout;
                        group.SetSessionLifetime(customSessionLifetime?.Add(TimeSpan.FromSeconds(1)) ?? TimeSpan.FromSeconds(6)); //todo (E006)
                        break;
                    case KafkaResponseErrorCode.RebalanceInProgress:
                        error = KafkaConsumerGroupErrorCode.Rebalance;
                        errorType = GroupErrorType.Rebalance;
                        break;
                    case KafkaResponseErrorCode.GroupAuthorizationFailed:
                        error = KafkaConsumerGroupErrorCode.GroupAuthorizationFailed;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    default:
                        error = KafkaConsumerGroupErrorCode.UnknownError;
                        errorType = GroupErrorType.Rearrange;
                        break;
                }
                
                HandleProtocolError(group, error, errorType, description);
                return false;
            }

            var isLeader = response.MemberId == response.GroupLeaderId;
            group.SetMemberData(response.GroupGenerationId, response.MemberId, isLeader);
            group.SetProtocolData(response.GroupProtocolName, null);
            
            if (!isLeader)
            {                
                return true;
            }            
                    
            // fill group members info that required for assignment process

            var topicMembers = new Dictionary<string, List<KafkaCoordinatorGroupMemberAssignmentData>>(group.Topics.Count);
            foreach (var topicPair in group.Topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                topicMembers[topic.TopicName] = new List<KafkaCoordinatorGroupMemberAssignmentData>();
            }

            var responseMembers = response.Members;
            var additionalTopics = new List<string>();            
            var groupMembers = new List<KafkaCoordinatorGroupMemberAssignmentData>();
            if (responseMembers != null)
            {
                foreach (var responseMember in responseMembers)
                {
                    if (responseMember?.MemberId == null) continue;

                    var isMemberLeader = responseMember.MemberId == response.GroupLeaderId;
                    var member = new KafkaCoordinatorGroupMemberAssignmentData(responseMember.MemberId, isMemberLeader,
                        responseMember.ProtocolVersion,
                        responseMember.AssignmentStrategies ?? new string[0], 
                        responseMember.CustomData);

                    foreach (var topicName in responseMember.TopicNames ?? new string[0])
                    {
                        if (string.IsNullOrEmpty(topicName)) continue;

                        List<KafkaCoordinatorGroupMemberAssignmentData> memberList;
                        if (!topicMembers.TryGetValue(topicName, out memberList) || memberList == null)
                        {
                            memberList = new List<KafkaCoordinatorGroupMemberAssignmentData>();
                            topicMembers[topicName] = memberList;
                            additionalTopics.Add(topicName);
                        }
                        memberList.Add(member);
                    }

                    groupMembers.Add(member);
                }
            }

            group.SetLeaderData(null, groupMembers, topicMembers, additionalTopics);            

            return true;
        }

        #endregion JoinGroup

        #region Additional topics

        private bool TryHandleAdditionalTopics([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaTopicMetadataResponse response, string description)
        {            
            var responseTopics = response.Topics ?? new KafkaTopicMetadataResponseTopic[0];            

            foreach (var responseTopic in responseTopics)
            {
                var topicName = responseTopic?.TopicName;
                if (string.IsNullOrEmpty(topicName)) continue;

                if (responseTopic.ErrorCode != KafkaResponseErrorCode.NoError)
                {
                    if (responseTopic.ErrorCode == KafkaResponseErrorCode.UnknownTopicOrPartition ||
                        responseTopic.ErrorCode == KafkaResponseErrorCode.InvalidTopic)
                    {
                        continue;
                    }

                    if (responseTopic.ErrorCode == KafkaResponseErrorCode.TopicAuthorizationFailed)
                    {
                        HandleProtocolError(group, KafkaConsumerGroupErrorCode.TopicAuthorizationFailed, GroupErrorType.Error, description);
                        return false;
                    }
                }

                var responsePartitons = responseTopic.Partitions;
                if (responsePartitons == null) continue;

                var partitions = new List<int>(responsePartitons.Count);
                foreach (var responsePartition in responsePartitons)
                {
                    if (responsePartition == null) continue;                    
                    if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        if (responsePartition.ErrorCode == KafkaResponseErrorCode.UnknownTopicOrPartition)
                        {
                            continue;
                        }
                    }
                    
                    partitions.Add(responsePartition.PartitionId);
                }
                group.TopicMetadataPartitionIds[topicName] = partitions;
            }                                   

            return true;
        }

        #endregion Additional topics

        #region Assignment

        private bool TryAssignTopics([NotNull] KafkaCoordinatorGroup group, string description)
        {
            var leaderData = group.LeaderData;
            if (leaderData == null)
            {
                HandleProtocolError(group, KafkaConsumerGroupErrorCode.ClientError, GroupErrorType.Error, $"{description}(no members data)");
                return false;
            }
            var groupMembers = leaderData.GroupMembers;
            var topicMembers = leaderData.TopicMembers;
            

            var currentProtocol = group.ProtocolData;
            var settingsProtocols = group.Protocols;            
            
            // prepare available protocol versions and strategies according to received protocol
            var supportedProtocolStrategiesByVersion = new Dictionary<short, IReadOnlyList<KafkaConsumerAssignmentStrategyInfo>>(settingsProtocols.Count);
            var supportedProtocolVersions = new List<short>(settingsProtocols.Count);
            foreach (var settingsProtocol in settingsProtocols)
            {                             
                if (settingsProtocol.ProtocolName == currentProtocol?.ProtocolName)
                {                    
                    supportedProtocolVersions.Add(settingsProtocol.ProtocolVersion);
                    supportedProtocolStrategiesByVersion[settingsProtocol.ProtocolVersion] = settingsProtocol.AssignmentStrategies;
                }
            }
            if (supportedProtocolStrategiesByVersion.Count == 0)
            {
                HandleProtocolError(group, KafkaConsumerGroupErrorCode.AssignmentError, GroupErrorType.Error, $"{description}(no supported strategies)");
                return false;
            }
            supportedProtocolVersions.Sort();
            supportedProtocolVersions.Reverse();

            // aggregate requirements - min supported protocol version and set of supported strategies
            short? minMembersProtocolVersion = null;
            HashSet<string> supportedStrategies = null;
            var groupMembersDictionary = new Dictionary<string, KafkaCoordinatorGroupMemberAssignmentData>(groupMembers.Count);
            foreach (var groupMember in groupMembers)
            {
                if (groupMember == null) continue;

                groupMembersDictionary[groupMember.MemberId] = groupMember;

                if (minMembersProtocolVersion == null || minMembersProtocolVersion.Value > groupMember.ProtocolVersion)
                {
                    minMembersProtocolVersion = groupMember.ProtocolVersion;
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
            if (minMembersProtocolVersion == null)
            {
                HandleProtocolError(group, KafkaConsumerGroupErrorCode.AssignmentError, GroupErrorType.Error, $"{description}(strategies are not consistent)");
                return false;
            }            

            // find the most relevant protocol
            short? selectedProtocolVersion = null;
            foreach (var groupProtocolVersion in supportedProtocolVersions)
            {
                if (groupProtocolVersion <= minMembersProtocolVersion)
                {
                    selectedProtocolVersion = groupProtocolVersion;
                    break;
                }
            }
            if (selectedProtocolVersion == null)
            {
                selectedProtocolVersion = supportedProtocolVersions[0];
            }

            // find the most relevant assignment strategy
            var selectedProtocolStrategies = supportedProtocolStrategiesByVersion[selectedProtocolVersion.Value];
            KafkaConsumerAssignmentStrategyInfo selectedStrategy = null;
            if (selectedProtocolStrategies != null)
            {
                foreach (var strategy in selectedProtocolStrategies)
                {
                    if (strategy?.StrategyName == null) continue;

                    if (supportedStrategies.Contains(strategy.StrategyName))
                    {
                        selectedStrategy = strategy;
                        break;
                    }
                }
                if (selectedStrategy == null)
                {
                    selectedStrategy = selectedProtocolStrategies[0];
                }
            }            

            // assign topic partitions on members
            foreach (var topicMember in topicMembers)
            {
                var topicName = topicMember.Key;
                var members = topicMember.Value;
                if (members == null || members.Count == 0 || topicName == null) continue;

                // check topic is known
                IReadOnlyList<int> partitionIds;
                if (!group.TopicMetadataPartitionIds.TryGetValue(topicName, out partitionIds) || partitionIds == null)
                {
                    continue;
                }

                // preprae members for topic
                var assignmentRequestMembers = new List<KafkaConsumerAssignmentRequestMember>(members.Count);
                foreach (var member in members)
                {
                    if (member == null) continue;
                    assignmentRequestMembers.Add(new KafkaConsumerAssignmentRequestMember(member.MemberId, member.IsLeader, member.CustomData));
                }

                // assign topic via current assignment strategy
                var assignmentRequest = new KafkaConsumerAssignmentRequest(topicName, partitionIds, assignmentRequestMembers);
                KafkaConsumerAssignment assignment;
                try
                {
                    assignment = selectedStrategy?.Strategy?.Assign(assignmentRequest);
                }
                catch (Exception exception)
                {
                    LogAssignmentError(group, assignmentRequest, exception);
                    continue;
                }

                // apply assignment to members
                var assignmentMembers = assignment?.Members;
                if (assignmentMembers == null)
                {
                    LogAssignmentError(group, assignmentRequest, null);
                    continue;
                }

                foreach (var assignmentMember in assignmentMembers)
                {
                    if (assignmentMember == null) continue;
                    var assignmentMemberId = assignmentMember.MemberId;
                    var assignmentPartitionIds = assignmentMember.PartitionIds;
                    if (assignmentMemberId == null || assignmentPartitionIds == null || assignmentPartitionIds.Count == 0) continue;

                    KafkaCoordinatorGroupMemberAssignmentData groupMember;
                    if (!groupMembersDictionary.TryGetValue(assignmentMemberId, out groupMember) || groupMember == null) continue;

                    groupMember.TopicAssignments[topicName] = assignmentPartitionIds;
                }
            }

            group.SetProtocolData(currentProtocol?.ProtocolName, selectedProtocolVersion.Value);
            group.SetLeaderData(selectedStrategy?.StrategyName, leaderData.GroupMembers, leaderData.TopicMembers, leaderData.AdditionalTopicNames);

            return true;
        }

        #endregion Assignment

        #region SyncGroup

        [NotNull]
        private KafkaSyncGroupRequest CreateSyncGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.MemberData;
            var groupGenerationId = sessionInfo?.GenerationId ?? -DefaultGenerationId;
            var groupMemberId = sessionInfo?.MemberId ?? DefaultMemberId;            

            var groupMembers = group.LeaderData?.GroupMembers;

            if (groupMembers == null)
            {
                return new KafkaSyncGroupRequest(group.GroupName, groupGenerationId, groupMemberId, new KafkaSyncGroupRequestMember[0]);
            }

            var requestMembers = new List<KafkaSyncGroupRequestMember>(groupMembers.Count);
            
            foreach (var groupMember in groupMembers)
            {
                if (groupMember == null) continue;

                var groupMemberTopics = new List<KafkaSyncGroupRequestMemberTopic>(groupMember.TopicAssignments.Count);
                foreach (var topicAssignment in groupMember.TopicAssignments)
                {
                    var topicName = topicAssignment.Key;
                    var topicPartitionIds = topicAssignment.Value;
                    if (string.IsNullOrEmpty(topicName) || topicPartitionIds == null || topicPartitionIds.Count == 0) continue;

                    var gropMemberTopic = new KafkaSyncGroupRequestMemberTopic(topicName, topicPartitionIds);
                    groupMemberTopics.Add(gropMemberTopic);
                }
                var requestMember = new KafkaSyncGroupRequestMember(groupMember.MemberId, group.ProtocolData?.ProtocolVersion ?? DefaultProtocolVersion, groupMemberTopics, groupMember.CustomData);
                requestMembers.Add(requestMember);
            }

            return new KafkaSyncGroupRequest(group.GroupName, groupGenerationId, groupMemberId, requestMembers);
        }

        private bool TryHandleSyncGroupResponse([NotNull] KafkaCoordinatorGroup group, [NotNull]KafkaSyncGroupResponse response, string description)
        {
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaConsumerGroupErrorCode error;
                GroupErrorType errorType;
                switch (response.ErrorCode)
                {
                    case KafkaResponseErrorCode.GroupLoadInProgress:
                        error = KafkaConsumerGroupErrorCode.GroupLoadInProgress;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                        error = KafkaConsumerGroupErrorCode.GroupCoordinatorNotAvailable;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotCoordinatorForGroup:
                        error = KafkaConsumerGroupErrorCode.NotCoordinatorForGroup;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.IllegalGeneration:
                        error = KafkaConsumerGroupErrorCode.Rebalance;
                        errorType = GroupErrorType.Rebalance;
                        break;                    
                    case KafkaResponseErrorCode.UnknownMemberId:
                        error = KafkaConsumerGroupErrorCode.UnknownMemberId;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.RebalanceInProgress:
                        error = KafkaConsumerGroupErrorCode.Rebalance;
                        errorType = GroupErrorType.Rebalance;
                        break;
                    case KafkaResponseErrorCode.GroupAuthorizationFailed:
                        error = KafkaConsumerGroupErrorCode.GroupAuthorizationFailed;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    default:
                        error = KafkaConsumerGroupErrorCode.UnknownError;
                        errorType = GroupErrorType.Rearrange;
                        break;
                }

                HandleProtocolError(group, error, errorType, description);
                return false;
            }

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

            group.SetProtocolData(group.ProtocolData?.ProtocolName, response.ProtocolVersion);
            group.SetAssignmentData(memberAssignment);

            return true;
        }

        #endregion SyncGroup

        #region Heartbeat

        [NotNull]
        private KafkaHeartbeatRequest CreateHeartbeatRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.MemberData;            
            return new KafkaHeartbeatRequest(group.GroupName, sessionInfo?.GenerationId ?? DefaultGenerationId, sessionInfo?.MemberId ?? DefaultMemberId);
        }

        private bool TryHandleHeartbeatResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaHeartbeatResponse response, string description)
        {
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaConsumerGroupErrorCode error;
                GroupErrorType errorType;
                switch (response.ErrorCode)
                {
                    case KafkaResponseErrorCode.GroupLoadInProgress:
                        error = KafkaConsumerGroupErrorCode.GroupLoadInProgress;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                        error = KafkaConsumerGroupErrorCode.GroupCoordinatorNotAvailable;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotCoordinatorForGroup:
                        error = KafkaConsumerGroupErrorCode.NotCoordinatorForGroup;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.IllegalGeneration:
                        error = KafkaConsumerGroupErrorCode.Rebalance;
                        errorType = GroupErrorType.Rebalance;
                        break;
                    case KafkaResponseErrorCode.UnknownMemberId:
                        error = KafkaConsumerGroupErrorCode.UnknownMemberId;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.RebalanceInProgress:
                        error = KafkaConsumerGroupErrorCode.Rebalance;
                        errorType = GroupErrorType.Rebalance;
                        break;
                    case KafkaResponseErrorCode.GroupAuthorizationFailed:
                        error = KafkaConsumerGroupErrorCode.GroupAuthorizationFailed;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    default:
                        error = KafkaConsumerGroupErrorCode.UnknownError;
                        errorType = GroupErrorType.Rearrange;
                        break;
                }

                HandleProtocolError(group, error, errorType, description);
                return false;
            }

            return true;
        }
        #endregion Heartbeat

        #region OffsetFetch

        [NotNull]
        private KafkaOffsetFetchRequest CreateOffsetFetchRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var memberAssignment = group.AssignmentData?.AssignedTopicPartitions ?? new Dictionary<string, IReadOnlyList<int>>();

            var topics = new List<KafkaOffsetFetchRequestTopic>(memberAssignment.Count);
            foreach (var memberTopicAssignment in memberAssignment)
            {
                var topicName = memberTopicAssignment.Key;
                var topicPartitionIds = memberTopicAssignment.Value;

                topics.Add(new KafkaOffsetFetchRequestTopic(topicName, topicPartitionIds));
            }

            return new KafkaOffsetFetchRequest(group.GroupName, topics);
        }

        private bool TryHandleOffsetFetchResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaOffsetFetchResponse response, string description)
        {            
            var assignmentTopics = group.AssignmentData?.AssignedTopicPartitions;
            var responseTopics = response.Topics;
            if (responseTopics == null || responseTopics.Count == 0 || assignmentTopics == null)
            {
                return true;
            }            
            
            var topicPartitionOffsets = new Dictionary<string, KafkaCoordinatorGroupOffsetsDataTopic>(responseTopics.Count);

            // fill fetched offsets
            foreach (var responseTopic in responseTopics)
            {
                if (responseTopic == null) continue;                
                var topicName = responseTopic.TopicName;
                var responseTopicPartitions = responseTopic.Partitions;
                if (string.IsNullOrEmpty(topicName) || responseTopicPartitions == null) continue;
                
                IReadOnlyList<int> assignedTopicPartitions;
                if (!assignmentTopics.TryGetValue(topicName, out assignedTopicPartitions) || assignedTopicPartitions == null) continue;

                var assignedPartitionSet = new HashSet<int>(assignedTopicPartitions);

                var partitionOffsets = new Dictionary<int, KafkaCoordinatorGroupOffsetsDataPartition>(assignedTopicPartitions.Count);

                // fill ofests for fetched partitions
                foreach (var responsePartition in responseTopicPartitions)
                {
                    if (responsePartition == null) continue;                    
                    if (!assignedPartitionSet.Contains(responsePartition.PartitionId)) continue;

                    if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        KafkaConsumerGroupErrorCode error;
                        GroupErrorType errorType;
                        switch (responsePartition.ErrorCode)
                        {
                            case KafkaResponseErrorCode.GroupLoadInProgress:
                                error = KafkaConsumerGroupErrorCode.GroupLoadInProgress;
                                errorType = GroupErrorType.Error;
                                break;
                            case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                                error = KafkaConsumerGroupErrorCode.GroupCoordinatorNotAvailable;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.NotCoordinatorForGroup:
                                error = KafkaConsumerGroupErrorCode.NotCoordinatorForGroup;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.IllegalGeneration:
                                error = KafkaConsumerGroupErrorCode.Rebalance;
                                errorType = GroupErrorType.Rebalance;
                                break;
                            case KafkaResponseErrorCode.UnknownMemberId:
                                error = KafkaConsumerGroupErrorCode.UnknownMemberId;
                                errorType = GroupErrorType.Error;
                                break;                            
                            case KafkaResponseErrorCode.TopicAuthorizationFailed:
                                error = KafkaConsumerGroupErrorCode.TopicAuthorizationFailed;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.GroupAuthorizationFailed:
                                error = KafkaConsumerGroupErrorCode.GroupAuthorizationFailed;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            default:
                                error = KafkaConsumerGroupErrorCode.UnknownError;
                                errorType = GroupErrorType.Rearrange;
                                break;
                        }

                        HandleProtocolError(group, error, errorType, description);
                        return false;
                    }

                    var initialOffset = responsePartition.Offset;

                    partitionOffsets[responsePartition.PartitionId] = new KafkaCoordinatorGroupOffsetsDataPartition(initialOffset, initialOffset, DateTime.UtcNow);                    
                }

                topicPartitionOffsets[topicName] = new KafkaCoordinatorGroupOffsetsDataTopic(partitionOffsets);
            }

            group.SetOffsetsData(topicPartitionOffsets);

            return true;
        }

        #endregion OffsetFetch

        #region OffsetCommit

        [CanBeNull]
        private KafkaOffsetCommitRequest CreateOffsetCommitRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var offsets = group.OffsetsData;
            if (offsets == null || offsets.Topics.Count == 0) return null;

            var requestTopics = new List<KafkaOffsetCommitRequestTopic>(offsets.Topics.Count);

            foreach (var groupTopicPair in group.Topics)
            {
                var groupTopic = groupTopicPair.Value;
                if (groupTopic == null) continue;
                var topicName = groupTopic.TopicName;

                KafkaCoordinatorGroupOffsetsDataTopic topicOffsets;
                if (!offsets.Topics.TryGetValue(topicName, out topicOffsets) || topicOffsets == null)
                {
                    continue;
                }
              
                var requestPartitions = new List<KafkaOffsetCommitRequestTopicPartition>(topicOffsets.Partitions.Count);
                foreach (var partitonPair in topicOffsets.Partitions)
                {
                    var partitionId = partitonPair.Key;
                    var partitionOffsets = partitonPair.Value;
                    if (partitionOffsets == null) continue;

                    var newClientOffset = groupTopic.Consumer?.GetCommitClientOffset(partitionId);
                    if (newClientOffset == null) continue;
                    
                    if (partitionOffsets.GroupServerOffset >= newClientOffset.Value) continue;
                    partitionOffsets.GroupClientOffset = newClientOffset.Value;
                    partitionOffsets.TimestampUtc = DateTime.UtcNow;

                    requestPartitions.Add(new KafkaOffsetCommitRequestTopicPartition(partitionId, newClientOffset.Value, group.CommitMetadata));                    
                }
                if (requestPartitions.Count == 0) continue;                
                requestTopics.Add(new KafkaOffsetCommitRequestTopic(topicName, requestPartitions));
            }
            if (requestTopics.Count == 0) return null;

            var sessionInfo = group.MemberData;
            return new KafkaOffsetCommitRequest(group.GroupName, sessionInfo?.GenerationId ?? - 1, sessionInfo?.MemberId, group.Settings.OffsetCommitRetentionTime, requestTopics);
        }

        private bool TryHandleOffsetCommitResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaOffsetCommitResponse response, string description)
        {            
            var offsets = group.OffsetsData?.Topics;
            
            var responseTopics = response.Topics;
            if (responseTopics == null || responseTopics.Count == 0 || offsets == null)
            {
                return true;
            }

            var hasError = false;     

            // fill fetched offsets
            foreach (var responseTopic in responseTopics)
            {
                if (responseTopic == null) continue;
                var topicName = responseTopic.TopicName;
                var responseTopicPartitions = responseTopic.Partitions;
                if (string.IsNullOrEmpty(topicName) || responseTopicPartitions == null) continue;

                KafkaCoordinatorGroupOffsetsDataTopic topicOffsets;
                if (!offsets.TryGetValue(topicName, out topicOffsets) || topicOffsets == null)
                {
                    continue;
                }               
                
                foreach (var responsePartition in responseTopicPartitions)
                {
                    if (responsePartition == null) continue;

                    KafkaCoordinatorGroupOffsetsDataPartition partitionOffsets;
                    if (!topicOffsets.Partitions.TryGetValue(responsePartition.PartitionId, out partitionOffsets) ||
                        partitionOffsets == null)
                    {
                        continue;
                    }

                    if (responsePartition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        if (hasError)
                        {
                            continue;
                        }

                        KafkaConsumerGroupErrorCode error;
                        GroupErrorType errorType;
                        switch (responsePartition.ErrorCode)
                        {
                            case KafkaResponseErrorCode.OffsetMetadataTooLarge:
                                error = KafkaConsumerGroupErrorCode.OffsetMetadataTooLarge;
                                if (group.CommitMetadata != null)
                                {
                                    group.SetCommitMetadata(null);
                                    errorType = GroupErrorType.Warning;
                                }
                                else
                                {
                                    errorType = GroupErrorType.Error;
                                }
                                break;
                            case KafkaResponseErrorCode.GroupLoadInProgress:
                                error = KafkaConsumerGroupErrorCode.GroupLoadInProgress;
                                errorType = GroupErrorType.Error;
                                break;
                            case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                                error = KafkaConsumerGroupErrorCode.GroupCoordinatorNotAvailable;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.NotCoordinatorForGroup:
                                error = KafkaConsumerGroupErrorCode.NotCoordinatorForGroup;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.IllegalGeneration:
                                error = KafkaConsumerGroupErrorCode.Rebalance;
                                errorType = GroupErrorType.Rebalance;
                                break;
                            case KafkaResponseErrorCode.UnknownMemberId:
                                error = KafkaConsumerGroupErrorCode.UnknownMemberId;
                                errorType = GroupErrorType.Error;
                                break;
                            case KafkaResponseErrorCode.RebalanceInProgress:
                                error = KafkaConsumerGroupErrorCode.Rebalance;
                                errorType = GroupErrorType.Rebalance;
                                break;
                            case KafkaResponseErrorCode.InvalidCommitOffsetSize:
                                error = KafkaConsumerGroupErrorCode.InvalidCommitOffsetSize;
                                if (group.CommitMetadata != null)
                                {
                                    group.SetCommitMetadata(null);
                                    errorType = GroupErrorType.Warning;
                                }
                                else
                                {
                                    errorType = GroupErrorType.Error;
                                }
                                break;
                            case KafkaResponseErrorCode.TopicAuthorizationFailed:
                                error = KafkaConsumerGroupErrorCode.TopicAuthorizationFailed;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            case KafkaResponseErrorCode.GroupAuthorizationFailed:
                                error = KafkaConsumerGroupErrorCode.GroupAuthorizationFailed;
                                errorType = GroupErrorType.Rearrange;
                                break;
                            default:
                                error = KafkaConsumerGroupErrorCode.UnknownError;
                                errorType = GroupErrorType.Rearrange;
                                break;
                        }

                        HandleProtocolError(group, error, errorType, description);
                        hasError = true;
                        continue; // don't return! may be part of topics is successfull.
                    }

                    partitionOffsets.GroupServerOffset = partitionOffsets.GroupClientOffset;
                    partitionOffsets.TimestampUtc = DateTime.UtcNow;                    
                }
            }

            return !hasError;
        }

        #endregion OffsetCommit

        #region LeaveGroup

        [CanBeNull]
        private KafkaLeaveGroupRequest CreateLeaveGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.MemberData;
            if (sessionInfo == null) return null;
            return new KafkaLeaveGroupRequest(group.GroupName, sessionInfo.MemberId ?? DefaultMemberId);
        }

        #endregion LeaveGroup

        #region Error handling

        private void LogBrokerError([NotNull] KafkaCoordinatorGroup group, KafkaBrokerErrorCode brokerError, string errorDescription)
        {
            var logger = group.Logger;
            if (logger == null) return;

            var errorInfo = new KafkaConsumerGroupTransportErrorInfo(brokerError, errorDescription, _clientBroker);
            logger.OnTransportError(errorInfo);
        }

        private void LogProtocolError([NotNull] KafkaCoordinatorGroup group, KafkaConsumerGroupErrorCode error, GroupErrorType erorrType, string errorDescription)
        {
            var logger = group.Logger;
            if (logger == null) return;

            var errorInfo = new KafkaConsumerGroupProtocolErrorInfo(error, errorDescription, _clientBroker);
            if (erorrType == GroupErrorType.Warning)
            {
                logger.OnProtocolWarning(errorInfo);
                return;
            }

            if (erorrType == GroupErrorType.Rebalance)
            {
                logger.OnServerRebalance(errorInfo);
                return;
            }

            logger.OnProtocolError(errorInfo);
        }

        private void LogAssignmentError([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaConsumerAssignmentRequest request, [CanBeNull] Exception exception)
        {
            var logger = group.Logger;
            if (logger == null) return;

            var errorInfo = new KafkaConsumerGroupAssignmentErrorInfo(request, exception);
            logger.OnAssignmentError(errorInfo);            
        }

        private void ResetError([NotNull] KafkaCoordinatorGroup group)
        {            
            var error = group.Error;
            var errorTimestamp = group.ErrorTimestampUtc;
            group.ResetError();
            if (error == null) return;

            var errorInfo = new KafkaConsumerGroupErrorResetInfo(error.Value, errorTimestamp);
            group.Logger?.OnErrorReset(errorInfo);
        }

        private bool TrySendRequest<TRequest>([NotNull] KafkaCoordinatorGroup group, [NotNull] TRequest request,
            string description,
            [NotNull] Dictionary<string, int> requests,            
            TimeSpan timeout
            )
            where TRequest : class, IKafkaRequest
        {
            var requestResult = _broker.Send(request, group.GroupCoordinatorName, timeout);
            if (requestResult.HasError || requestResult.Data == null)
            {
                HandleBrokerError(group, requestResult.Error ?? KafkaBrokerErrorCode.TransportError, description);
                return false;
            }

            var requestId = requestResult.Data.Value;
            requests[group.GroupName] = requestId;
            return true;
        }

        private delegate bool KafkaHandleResponseMethod<in TResponse>(KafkaCoordinatorGroup group, TResponse response, string description) where TResponse : class, IKafkaResponse;            

        private bool TryHandleResponse<TResponse>([NotNull] KafkaCoordinatorGroup group, string description,
            [NotNull] Dictionary<string, int> requests,
            [NotNull] KafkaHandleResponseMethod<TResponse> handleMethod
            )
            where TResponse : class, IKafkaResponse
        {
            int requestId;
            if (!requests.TryGetValue(group.GroupName, out requestId))
            {                
                HandleProtocolError(group, KafkaConsumerGroupErrorCode.ClientError, GroupErrorType.Error, description);
                LogProtocolError(group, KafkaConsumerGroupErrorCode.ClientError, GroupErrorType.Warning, $"{description}(no request");
                return false;
            }

            var response = _broker.Receive<TResponse>(requestId);
            if (!response.HasData && !response.HasError) return false;

            requests.Remove(group.GroupName);

            if (response.Error != null || response.Data == null)
            {
                HandleBrokerError(group, response.Error ?? KafkaBrokerErrorCode.TransportError, description);
                return false;
            }

            return handleMethod(group, response.Data, description);
        }

        private void HandleBrokerError([NotNull] KafkaCoordinatorGroup group, KafkaBrokerErrorCode brokerError, string errorDescription)
        {
            KafkaConsumerGroupErrorCode sessionErrorCode;
            
            switch (brokerError)
            {
                case KafkaBrokerErrorCode.ConnectionClosed:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionMaintenance:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ClientMaintenance;
                    break;
                case KafkaBrokerErrorCode.BadRequest:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.ProtocolError:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TransportError:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.ClientTimeout:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ClientTimeout;
                    break;
                case KafkaBrokerErrorCode.Cancelled:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.ConnectionRefused:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.HostUnreachable:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.HostUnreachable;
                    break;
                case KafkaBrokerErrorCode.HostNotAvailable:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.HostNotAvailable;
                    break;
                case KafkaBrokerErrorCode.NotAuthorized:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.NotAuthorized;
                    break;
                case KafkaBrokerErrorCode.UnsupportedOperation:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.OperationRefused:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TooBigMessage:
                    // there are only command requests w/o data - network problem.
                    sessionErrorCode = KafkaConsumerGroupErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.UnknownError:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.UnknownError;
                    break;
                default:
                    sessionErrorCode = KafkaConsumerGroupErrorCode.UnknownError;
                    break;
            }

            group.SetError(sessionErrorCode);
            group.ResetData();
            group.ResetSettings();
            group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
            
            LogBrokerError(group, brokerError, errorDescription);
        }

        private void HandleProtocolError([NotNull] KafkaCoordinatorGroup group, KafkaConsumerGroupErrorCode errorCode,
            GroupErrorType errorType, string description)
        {
            group.SetError(errorCode);
            switch (errorType)
            {
                case GroupErrorType.Warning:
                    break;
                case GroupErrorType.Rebalance:
                    group.Status = KafkaCoordinatorGroupStatus.Rebalance;
                    var sessionData = group.MemberData;
                    group.ResetData();
                    if (sessionData != null)
                    {
                        group.SetMemberData(sessionData.GenerationId, sessionData.MemberId, sessionData.IsLeader);
                    }
                    break;
                case GroupErrorType.Error:
                    group.ResetData();
                    group.Status = KafkaCoordinatorGroupStatus.Error;
                    break;
                case GroupErrorType.Rearrange:
                    group.ResetData();
                    group.ResetSettings();
                    group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
                    break;
            }
            LogProtocolError(group, errorCode, errorType, description);
        }

        private enum GroupErrorType
        {
            Warning = 0,
            Rebalance = 1,
            Error = 2,
            Rearrange = 3
        }

        #endregion Error handling
    }
}