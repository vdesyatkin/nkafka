using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;
using NKafka.Connection;
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

        public KafkaCoordinatorBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod)
        {
            _broker = broker;
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

        public void Process()
        {
            foreach (var groupPair in _groups)
            {
                var group = groupPair.Value;
                if (group == null) continue;

                ProcessGroup(group);
            }
        }

        public void Close()
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
                        _broker.SendWithoutResponse(commitRequest);
                    }
                }

                if (group.Status >= KafkaCoordinatorGroupStatus.JoinGroupRequested)
                {
                    var leaveGroupRequest = CreateLeaveGroupRequest(group);
                    if (leaveGroupRequest != null)
                    {
                        _broker.SendWithoutResponse(leaveGroupRequest);
                    }
                }

                group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
            }

            _joinGroupRequests.Clear();
            _additionalTopicsRequests.Clear();
            _syncGroupRequests.Clear();
            _heartbeatRequests.Clear();
            _offsetFetchRequests.Clear();
            _offsetCommitRequests.Clear();
        }

        private void ProcessGroup([NotNull] KafkaCoordinatorGroup group)
        {
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
                    group.AllTopicPartitions[topic.TopicName] = partitionIds;
                }

                var joinRequest = CreateJoinGroupRequest(group);
                if (joinRequest == null) return;

                if (!TrySendRequest(group, joinRequest, _joinGroupRequests, _coordinatorClientTimeout + group.Settings.JoinGroupServerTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.JoinGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinGroupRequested)
            {                
                if (!TryHandleResponse<KafkaJoinGroupResponse>(group, _joinGroupRequests, TryHandleJoinGroupResponse))
                {
                    return;
                }

                if (group.SessionInfo?.IsLeader == true)
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
                if (!TrySendRequest(group, topicMetadataRequest, _additionalTopicsRequests, _coordinatorClientTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.AdditionalTopicsMetadataRequested)
            {
                if (!TryHandleResponse<KafkaTopicMetadataResponse>(group, _additionalTopicsRequests, TryHandleAdditionalTopics))
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
                if (!TrySendRequest(group, syncRequest, _syncGroupRequests, _coordinatorClientTimeout + group.Settings.SyncGroupServerTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.SyncGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.JoinedAsMember)
            {
                var syncRequest = CreateSyncGroupRequest(group);
                if (!TrySendRequest(group, syncRequest, _syncGroupRequests, _coordinatorClientTimeout + group.Settings.SyncGroupServerTimeout))
                {
                    return;
                }

                group.Status = KafkaCoordinatorGroupStatus.SyncGroupRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.SyncGroupRequested)
            {
                if (!TryHandleResponse<KafkaSyncGroupResponse>(group, _syncGroupRequests, TryHandleSyncGroupResponse))
                {
                    return;
                }                

                group.Status = KafkaCoordinatorGroupStatus.FirstHeatbeatRequired;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeatbeatRequired)
            {
                var heartbeatRequest = CreateHeartbeatRequest(group);
                if (!TrySendRequest(group, heartbeatRequest, _heartbeatRequests, _coordinatorClientTimeout + group.Settings.HeartbeatServerTimeout))
                {
                    return;
                }

                group.HeartbeatTimestampUtc = DateTime.UtcNow;
                group.Status = KafkaCoordinatorGroupStatus.FirstHeatbeatRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.FirstHeatbeatRequested)
            {
                if (!TryHandleResponse<KafkaHeartbeatResponse>(group, _heartbeatRequests, TryHandleHeartbeatResponse))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequired;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.Ready ||
                group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequired || group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequested)
            {
                //regular heartbeat

                
                if (_heartbeatRequests.ContainsKey(group.GroupName))
                {
                    if (!TryHandleResponse<KafkaHeartbeatResponse>(group, _heartbeatRequests, TryHandleHeartbeatResponse))
                    {
                        return;
                    }                    
                }

                if (group.HeartbeatTimestampUtc + group.HeartbeatPeriod >= DateTime.UtcNow &&
                    !_heartbeatRequests.ContainsKey(group.GroupName))
                {
                    var heartbeatRequest = CreateHeartbeatRequest(group);
                    if (!TrySendRequest(group, heartbeatRequest, _heartbeatRequests, _coordinatorClientTimeout + group.Settings.HeartbeatServerTimeout))
                    {
                        return;
                    }        

                    group.HeartbeatTimestampUtc = DateTime.UtcNow;                    
                }
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequired)
            {
                var offsetFetchRequest = CreateOffsetFetchRequest(group);
                if (!TrySendRequest(group, offsetFetchRequest, _offsetFetchRequests, _coordinatorClientTimeout + group.Settings.OffsetFetchServerTimeout))
                {
                    return;
                }
                
                group.Status = KafkaCoordinatorGroupStatus.OffsetFetchRequested;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.OffsetFetchRequested)
            {
                if (!TryHandleResponse<KafkaOffsetFetchResponse>(group, _offsetFetchRequests, TryHandleOffsetFetchResponse))
                {
                    return;
                }               

                group.CommitTimestampUtc = DateTime.UtcNow;
                group.Status = KafkaCoordinatorGroupStatus.Ready;
            }

            if (group.Status == KafkaCoordinatorGroupStatus.Ready)
            {
                //regular commit
                
                if (_offsetCommitRequests.ContainsKey(group.GroupName))
                {
                    if (!TryHandleResponse<KafkaOffsetCommitResponse>(group, _offsetCommitRequests,
                            TryHandleOffsetCommitResponse))
                    {
                        return;
                    }
                }

                if (group.CommitTimestampUtc + group.CommitPeriod >= DateTime.UtcNow &&
                    !_offsetCommitRequests.ContainsKey(group.GroupName))
                {
                    group.CommitTimestampUtc = DateTime.UtcNow;

                    var commitRequest = CreateOffsetCommitRequest(group);
                    if (commitRequest == null)
                    {
                        return;
                    }

                    if (!TrySendRequest(group, commitRequest, _offsetCommitRequests, _coordinatorClientTimeout + group.Settings.OffsetCommitServerTimeout))
                    {
                        return;
                    }
                    
                    group.Status = KafkaCoordinatorGroupStatus.Ready;               
                }
            }            
        }
        
        private bool TrySendRequest<TRequest>([NotNull] KafkaCoordinatorGroup group, [NotNull] TRequest request, [NotNull] Dictionary<string, int> requests, TimeSpan timeout)
            where TRequest: class, IKafkaRequest
        {
            var requestResult = _broker.Send(request, timeout);
            if (requestResult.HasError || requestResult.Data == null)
            {
                HandleBrokerError(group, requestResult.Error ?? KafkaBrokerErrorCode.UnknownError);
                return false;
            }

            var requestId = requestResult.Data.Value;
            requests[group.GroupName] = requestId;
            return true;
        }

        private bool TryHandleResponse<TResponse>([NotNull] KafkaCoordinatorGroup group, [NotNull] Dictionary<string, int> requests,
            [NotNull] Func<KafkaCoordinatorGroup, TResponse, bool> handleMethod) 
            where TResponse: class, IKafkaResponse
        {
            int requestId;
            if (!requests.TryGetValue(group.GroupName, out requestId))
            {
                SetGroupError(group, KafkaConsumerGroupSessionErrorCode.UnknownError, GroupErrorType.Error);
                return false;
            }

            var response = _broker.Receive<TResponse>(requestId);
            if (!response.HasData && !response.HasError) return false;

            _joinGroupRequests.Remove(group.GroupName);

            if (response.Error != null || response.Data == null)
            {
                HandleBrokerError(group, response.Error ?? KafkaBrokerErrorCode.TransportError);
                return false;
            }

            return handleMethod(group, response.Data);
        }

        private void HandleBrokerError([NotNull] KafkaCoordinatorGroup group, KafkaBrokerErrorCode errorCode)
        {
            KafkaConsumerGroupSessionErrorCode sessionErrorCode;
            
            switch (errorCode)
            {
                case KafkaBrokerErrorCode.Closed:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.ConnectionClosed;
                    break;
                case KafkaBrokerErrorCode.Maintenance:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.ClientMaintenance;
                    break;
                case KafkaBrokerErrorCode.BadRequest:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.ProtocolError:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.ProtocolError;
                    break;
                case KafkaBrokerErrorCode.TransportError:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.TransportError;
                    break;
                case KafkaBrokerErrorCode.Timeout:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.ClientTimeout;
                    break;
                default:
                    sessionErrorCode = KafkaConsumerGroupSessionErrorCode.UnknownError;
                    break;
            }
            
            SetGroupError(group, sessionErrorCode, GroupErrorType.Rearrange);
        }

        private void SetGroupError([NotNull] KafkaCoordinatorGroup group, KafkaConsumerGroupSessionErrorCode errorCode,
            GroupErrorType errorType)
        {
            group.SetError(errorCode);
            switch (errorType)
            {
                case GroupErrorType.Warning:
                    break;
                case GroupErrorType.Rebalance:
                    group.Status = KafkaCoordinatorGroupStatus.Rebalance;
                    break;
                case GroupErrorType.Error:
                    group.ResetSession();
                    group.ResetProtocol();
                    group.Status = KafkaCoordinatorGroupStatus.Error;
                    break;                
                case GroupErrorType.Rearrange:
                    group.ResetSession();
                    group.ResetProtocol();
                    group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
                    break;                
            }
        }

        private enum GroupErrorType
        {
            Warning = 0,
            Rebalance = 1,
            Error = 2,      
            Rearrange = 3
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

            var request = new KafkaJoinGroupRequest(group.GroupName, group.SessionInfo?.MemberId ?? string.Empty, group.Settings.GroupSessionLifetime, protocols);
            return request;
        }

        private bool TryHandleJoinGroupResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaJoinGroupResponse response)
        {            
            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                KafkaConsumerGroupSessionErrorCode error;
                GroupErrorType errorType;                
                switch (response.ErrorCode)
                {
                    //todo (E009)
                    case KafkaResponseErrorCode.GroupLoadInProgress:
                        error = KafkaConsumerGroupSessionErrorCode.GroupLoadInProgress;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.GroupCoordinatorNotAvailable:
                        error = KafkaConsumerGroupSessionErrorCode.GroupCoordinatorNotAvailable;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.NotCoordinatorForGroup:
                        error = KafkaConsumerGroupSessionErrorCode.NotCoordinatorForGroup;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    case KafkaResponseErrorCode.InconsistentGroupProtocol:
                        error = KafkaConsumerGroupSessionErrorCode.InconsistentGroupProtocol;
                        errorType = GroupErrorType.Error;
                        break;
                    case KafkaResponseErrorCode.UnknownMemberId:
                        error = KafkaConsumerGroupSessionErrorCode.UnknownMemberId;
                        errorType = GroupErrorType.Error;                        
                        break;
                    case KafkaResponseErrorCode.InvalidSessionTimeout:
                        error = KafkaConsumerGroupSessionErrorCode.InvalidSessionTimeout;
                        errorType = GroupErrorType.Warning;
                        //todo change session timeout
                        break;
                    case KafkaResponseErrorCode.GroupAuthorizationFailed:
                        error = KafkaConsumerGroupSessionErrorCode.GroupAuthorizationFailed;
                        errorType = GroupErrorType.Rearrange;
                        break;
                    default:
                        error = KafkaConsumerGroupSessionErrorCode.UnknownError;
                        errorType = GroupErrorType.Error;
                        break;
                }
                
                SetGroupError(group, error, errorType);
                return false;
            }

            var isLeader = response.MemberId == response.GroupLeaderId;
            group.SetSession(response.GroupGenerationId, response.MemberId, isLeader);
            group.SetProtocol(response.GroupProtocolName, null);
            
            if (!isLeader)
            {                
                return true;
            }            
                                     
            var responseMembers = response.Members;                

            var topicMembers = new Dictionary<string, List<KafkaCoordinatorGroupMember>>(group.Topics.Count);
            foreach (var topicPair in group.Topics)
            {
                var topic = topicPair.Value;
                if (topic == null) continue;

                topicMembers[topic.TopicName] = new List<KafkaCoordinatorGroupMember>();
            }
            var additionalTopics = new List<string>();

            var groupMembers = new List<KafkaCoordinatorGroupMember>();
            if (responseMembers != null)
            {
                foreach (var responseMember in responseMembers)
                {
                    if (responseMember?.MemberId == null) continue;

                    var isMemberLeader = responseMember.MemberId == response.GroupLeaderId;
                    var member = new KafkaCoordinatorGroupMember(responseMember.MemberId, isMemberLeader,
                        responseMember.ProtocolVersion,
                        responseMember.AssignmentStrategies ?? new string[0], 
                        responseMember.CustomData);

                    foreach (var topicName in responseMember.TopicNames ?? new string[0])
                    {
                        if (string.IsNullOrEmpty(topicName)) continue;

                        List<KafkaCoordinatorGroupMember> memberList;
                        if (!topicMembers.TryGetValue(topicName, out memberList) || memberList == null)
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

        private bool TryHandleAdditionalTopics([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaTopicMetadataResponse response)
        {            
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
                group.AllTopicPartitions[topicName] = partitions;
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

            var currentProtocol = group.ProtocolInfo;
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
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                return false;
            }
            supportedProtocolVersions.Sort();
            supportedProtocolVersions.Reverse();

            // aggregate requirements - min supported protocol version and set of supported strategies
            short? minMembersProtocolVersion = null;
            HashSet<string> supportedStrategies = null;
            var groupMembersDictionary = new Dictionary<string, KafkaCoordinatorGroupMember>(groupMembers.Count);
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
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
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
                if (!group.AllTopicPartitions.TryGetValue(topicName, out partitionIds) || partitionIds == null)
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
                catch (Exception)
                {
                    assignment = null;
                }

                // apply assignment to members
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
                    if (!groupMembersDictionary.TryGetValue(assignmentMemberId, out groupMember) || groupMember == null) continue;

                    groupMember.TopicAssignments[topicName] = assignmentPartitionIds;
                }
            }

            group.SetProtocol(currentProtocol?.ProtocolName, selectedProtocolVersion.Value);
            group.GroupAssignmentStrategyName = selectedStrategy?.StrategyName;

            return true;
        }

        #endregion Assignment

        #region SyncGroup

        [NotNull]
        private KafkaSyncGroupRequest CreateSyncGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.SessionInfo;
            var groupGenerationId = sessionInfo?.GenerationId ?? -DefaultGenerationId;
            var groupMemberId = sessionInfo?.MemberId ?? DefaultMemberId;

            var groupMembers = group.GroupMembers;

            if (groupMembers == null)
            {
                return new KafkaSyncGroupRequest(group.GroupName, groupGenerationId, groupMemberId, null);
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
                var requestMember = new KafkaSyncGroupRequestMember(groupMember.MemberId, group.ProtocolInfo?.ProtocolVersion ?? DefaultProtocolVersion, groupMemberTopics, groupMember.CustomData);
                requestMembers.Add(requestMember);
            }

            return new KafkaSyncGroupRequest(group.GroupName, groupGenerationId, groupMemberId, requestMembers);
        }

        private bool TryHandleSyncGroupResponse([NotNull] KafkaCoordinatorGroup group, [NotNull]KafkaSyncGroupResponse response)
        {            
            if (response.ErrorCode == KafkaResponseErrorCode.NotCoordinatorForGroup)
            {
                group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
                return false;
            }

            if (response.ErrorCode != KafkaResponseErrorCode.NoError)
            {
                //todo (E009)
                group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
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

            group.SetProtocol(group.ProtocolInfo?.ProtocolName, response.ProtocolVersion);
            group.AssignedTopicPartitions = memberAssignment;            

            return true;
        }

        #endregion SyncGroup

        #region Heartbeat

        [NotNull]
        private KafkaHeartbeatRequest CreateHeartbeatRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.SessionInfo;            
            return new KafkaHeartbeatRequest(group.GroupName, sessionInfo?.GenerationId ?? DefaultGenerationId, sessionInfo?.MemberId ?? DefaultMemberId);
        }

        private bool TryHandleHeartbeatResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaHeartbeatResponse response)
        {            
            if (response.ErrorCode == KafkaResponseErrorCode.NotCoordinatorForGroup)
            {
                group.Status = KafkaCoordinatorGroupStatus.RearrangeRequired;
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

        [NotNull]
        private KafkaOffsetFetchRequest CreateOffsetFetchRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var memberAssignment = group.AssignedTopicPartitions ?? new Dictionary<string, IReadOnlyList<int>>();

            var topics = new List<KafkaOffsetFetchRequestTopic>(memberAssignment.Count);
            foreach (var memberTopicAssignment in memberAssignment)
            {
                var topicName = memberTopicAssignment.Key;
                var topicPartitionIds = memberTopicAssignment.Value;

                topics.Add(new KafkaOffsetFetchRequestTopic(topicName, topicPartitionIds));
            }

            return new KafkaOffsetFetchRequest(group.GroupName, topics);
        }

        private bool TryHandleOffsetFetchResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaOffsetFetchResponse response)
        {            
            var assignment = group.AssignedTopicPartitions;
            var responseTopics = response.Topics;
            if (responseTopics == null || responseTopics.Count == 0 || assignment == null)
            {
                return true;
            }

            // null offset for all assignment topics/partitions
            var topicPartitionOffsets = new Dictionary<string, IReadOnlyDictionary<int, long?>>(responseTopics.Count);
            foreach (var topicAssignment in assignment)
            {
                var topicName = topicAssignment.Key;
                if (topicName == null) continue;
                var topicAssignmentPartitionIds = topicAssignment.Value;
                if (topicAssignmentPartitionIds == null) continue;

                var partitionOffsets = new Dictionary<int, long?>(topicAssignmentPartitionIds.Count);
                foreach (var partitionId in topicAssignmentPartitionIds)
                {
                    partitionOffsets[partitionId] = null;
                }
                topicPartitionOffsets[topicName] = partitionOffsets;
            }

            // fill fetched offsets
            foreach (var responseTopic in responseTopics)
            {
                if (responseTopic == null) continue;
                var topicName = responseTopic.TopicName;
                var topicPartitions = responseTopic.Partitions;
                if (string.IsNullOrEmpty(topicName) || topicPartitions == null) continue;

                IReadOnlyList<int> topicAssignment;
                if (!assignment.TryGetValue(topicName, out topicAssignment) || topicAssignment == null) continue;

                // fill null for not-fetched partitions
                var partitionOffsets = new Dictionary<int, long?>(topicPartitions.Count);
                foreach (var partitionId in topicAssignment)
                {
                    partitionOffsets[partitionId] = null;
                }

                // fill ofests for fetched partitions
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
                        continue;                        
                    }
                    partitionOffsets[partition.PartitionId] = partition.Offset;
                }

                topicPartitionOffsets[topicName] = partitionOffsets;
            }

            group.AssignedTopicPartitionOffsets = topicPartitionOffsets;

            foreach (var topicPair in topicPartitionOffsets)
            {
                var topicPartitions = topicPair.Value;
                if (topicPair.Key == null || topicPartitions == null) continue;

                var topicInitialCommits = new Dictionary<int, long?>(topicPartitions.Count);
                foreach (var partition in topicPartitions)
                {
                    topicInitialCommits[partition.Key] = partition.Value;
                }
                group.CommitedTopicPartitionOffsets[topicPair.Key] = topicInitialCommits;
            }

            return true;
        }

        #endregion OffsetFetch

        #region OffsetCommit

        [CanBeNull]
        private KafkaOffsetCommitRequest CreateOffsetCommitRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var commitOffsets = group.CommitedTopicPartitionOffsets;            

            var requestTopics = new List<KafkaOffsetCommitRequestTopic>(commitOffsets.Count);

            foreach (var groupTopic in group.Topics)
            {
                if (groupTopic.Value == null) continue;
                var topicName = groupTopic.Value.TopicName;                

                Dictionary<int, long?> commitPartitions;
                if (!commitOffsets.TryGetValue(topicName, out commitPartitions) || commitPartitions == null)
                {
                    continue;
                }                

                var requestPartitions = new List<KafkaOffsetCommitRequestTopicPartition>(commitPartitions.Count);
                foreach (var partitonPair in commitPartitions)
                {
                    var partitionId = partitonPair.Key;
                    var commitOffset = groupTopic.Value.Consumer?.GetCommitOffset(partitionId);
                    if (commitOffset == null) continue;
                    
                    requestPartitions.Add(new KafkaOffsetCommitRequestTopicPartition(partitionId, commitOffset.Value, group.Settings.OffsetCommitCustomData));                    
                }
                if (requestPartitions.Count == 0) continue;

                foreach (var requestPartition in requestPartitions)
                {
                    if (requestPartition == null) continue;
                    commitPartitions[requestPartition.PartitionId] = requestPartition.Offset;
                }

                requestTopics.Add(new KafkaOffsetCommitRequestTopic(topicName, requestPartitions));
            }
            if (requestTopics.Count == 0) return null;

            var sessionInfo = group.SessionInfo;
            return new KafkaOffsetCommitRequest(group.GroupName, sessionInfo?.GenerationId ?? - 1, sessionInfo?.MemberId, group.Settings.OffsetCommitRetentionTime, requestTopics);
        }

        private bool TryHandleOffsetCommitResponse([NotNull] KafkaCoordinatorGroup group, [NotNull] KafkaOffsetCommitResponse response)
        {            
            var commitedOffsets = group.CommitedTopicPartitionOffsets;
            var topics = response.Topics;
            if (topics == null || topics.Count == 0)
            {
                return true;
            }           

            // fill fetched offsets
            foreach (var topic in topics)
            {
                if (topic == null) continue;
                var topicName = topic.TopicName;
                var topicPartitions = topic.Partitions;
                if (string.IsNullOrEmpty(topicName) || topicPartitions == null) continue;

                Dictionary<int, long?> commitedPartitions;
                if (!commitedOffsets.TryGetValue(topicName, out commitedPartitions) || commitedPartitions == null)
                {
                    continue;
                }

                KafkaClientTopic groupTopic;
                if (!group.Topics.TryGetValue(topicName, out groupTopic))
                {
                    continue;
                }
                
                foreach (var partition in topicPartitions)
                {
                    if (partition == null) continue;
                    long? offset;
                    if (!commitedPartitions.TryGetValue(partition.PartitionId, out offset)) continue;

                    if (partition.ErrorCode == KafkaResponseErrorCode.NotCoordinatorForGroup)
                    {
                        group.Status = KafkaCoordinatorGroupStatus.NotInitialized;
                        return false;
                    }
                    if (partition.ErrorCode != KafkaResponseErrorCode.NoError)
                    {
                        //todo (E009)
                        continue;
                    }

                    if (offset.HasValue)
                    {
                        groupTopic?.Consumer?.ApproveCommitOffset(partition.PartitionId, offset.Value);
                    }
                }
            }

            return true;
        }

        #endregion OffsetCommit

        #region LeaveGroup

        [CanBeNull]
        private KafkaLeaveGroupRequest CreateLeaveGroupRequest([NotNull] KafkaCoordinatorGroup group)
        {
            var sessionInfo = group.SessionInfo;
            if (sessionInfo == null) return null;
            return new KafkaLeaveGroupRequest(group.GroupName, sessionInfo.MemberId ?? DefaultMemberId);
        }
      
        #endregion LeaveGroup
    }
}