using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;
        [NotNull, ItemNotNull] public readonly IReadOnlyDictionary<string, KafkaClientTopic> Topics;
        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;        
        

        public KafkaCoordinatorGroupStatus Status;
        public KafkaConsumerGroupSessionErrorCode? Error { get; private set; }
        public DateTime ErrorTimestampUtc { get; private set; }

        [CanBeNull] public KafkaCoordinatorGroupSessionData SessionData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupProtocolData ProtocolData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupLeaderData LeaderData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupAssignmentData AssignmentData { get; private set; }
        [CanBeNull] public KafkaCoordinatorGroupOffsetsData OffsetsData { get; private set; }

        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicMetadataPartitionIds;
       
        public DateTime HeartbeatTimestampUtc;
        public readonly TimeSpan HeartbeatPeriod;

        public DateTime CommitTimestampUtc;
        public readonly TimeSpan CommitPeriod;

        public KafkaCoordinatorGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;            
            TopicMetadataPartitionIds = new Dictionary<string, IReadOnlyList<int>>();            
            Settings = settings;

            var heartbeatPeriod = TimeSpan.FromSeconds(settings.GroupSessionLifetime.TotalSeconds/2 - 1);
            if (settings.HeartbeatPeriod < heartbeatPeriod)
            {
                heartbeatPeriod = settings.HeartbeatPeriod;
            }
            if (heartbeatPeriod < TimeSpan.FromMilliseconds(100))
            {
                heartbeatPeriod = TimeSpan.FromMilliseconds(100); //todo (E006)
            }
            HeartbeatPeriod = heartbeatPeriod;
            
            CommitPeriod = settings.OffsetCommitPeriod;

            var topicsDictionary = new Dictionary<string, KafkaClientTopic>(topics.Count);
            foreach (var topic in topics)
            {
                topicsDictionary[topic.TopicName] = topic;
            }
            Topics = topicsDictionary;

            var protocols = new List<KafkaConsumerGroupSettingsProtocol>();
            var settingsProtocols = settings.Protocols;
            if (settingsProtocols != null)
            {
                foreach (var settingsProtocol in settingsProtocols)
                {
                    if (settingsProtocol?.ProtocolName == null) continue;
                    var settingsStrategies = settingsProtocol.AssignmentStrategies;
                    if (settingsStrategies == null || settingsStrategies.Count == 0)
                    {
                        continue;
                    }

                    var strategies = new List<KafkaConsumerAssignmentStrategyInfo>(settingsStrategies.Count);
                    foreach (var settingsStrategy in settingsStrategies)
                    {
                        if (settingsStrategy?.StrategyName == null || settingsStrategy.Strategy == null) continue;

                        strategies.Add(settingsStrategy);
                    }
                    if (strategies.Count == 0) continue;

                    var protocol = new KafkaConsumerGroupSettingsProtocol(settingsProtocol.ProtocolName,
                        settingsProtocol.ProtocolVersion, strategies, settingsProtocol.CustomData);
                    protocols.Add(protocol);
                }
            }

            if (protocols.Count == 0)
            {
                protocols.Add(KafkaConsumerGroupSettingsBuilder.DefaultProtocol);
            }
            Protocols = protocols;
        }

        [CanBeNull] public IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets([NotNull] string topicName)
        {
            if (Status != KafkaCoordinatorGroupStatus.Ready) return null;

            var topics = OffsetsData?.Topics;
            if (topics == null) return null;

            KafkaCoordinatorGroupOffsetsDataTopic topic;
            if (topics.TryGetValue(topicName, out topic) && topic != null)
            {
                return topic.PartitionsReadOnly;
            }

            return null;
        }

        [NotNull]
        public KafkaConsumerGroupSessionInfo GetSessionDiagnosticsInfo()
        {
            var sessionData = SessionData;
            var protocolData = ProtocolData;
            var leaderData = LeaderData;

            return new KafkaConsumerGroupSessionInfo(GroupName, ErrorTimestampUtc,
                false, //todo 
                KafkaConsumerGroupSessionStatus.ToDo, //todo 
                KafkaConsumerGroupSessionErrorCode.UnknownError, //todo                
                new KafkaConsumerGroupSessionTopicInfo[0], //todo
                new KafkaConsumerGroupSessionProtocolInfo(protocolData?.ProtocolName, protocolData?.ProtocolVersion, leaderData?.AssignmentStrategyName, null), //todo
                new KafkaConsumerGroupSessionMemberInfo(sessionData?.GenerationId, sessionData?.MemberId, sessionData?.IsLeader ?? false)
                );
        }

        public void SetError(KafkaConsumerGroupSessionErrorCode errorCode)
        {
            ErrorTimestampUtc = DateTime.UtcNow;
            Error = errorCode;            
        }

        public void ResetError()
        {
            ErrorTimestampUtc = DateTime.UtcNow;
            Error = null;
        }

        public void SetSessionData(int generationId, string memberId, bool isLeader)
        {
            SessionData = new KafkaCoordinatorGroupSessionData(generationId, memberId, isLeader, DateTime.UtcNow);
        }   

        public void SetProtocolData(string protocolName, short? protocolVersion)
        {
            ProtocolData = new KafkaCoordinatorGroupProtocolData(protocolName, protocolVersion, DateTime.UtcNow);
        }

        public void SetLeaderData([CanBeNull] string assignmentStrategyName,
            [NotNull] IReadOnlyList<KafkaCoordinatorGroupMemberData> groupMembers,
            [NotNull] IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMemberData>> topicMembers,
            [NotNull] IReadOnlyList<string> additionalTopicNames)
        {
            LeaderData = new KafkaCoordinatorGroupLeaderData(assignmentStrategyName, groupMembers, topicMembers, additionalTopicNames, DateTime.UtcNow);
        }

        public void SetAssignmentData([NotNull] IReadOnlyDictionary<string, IReadOnlyList<int>> topicPartitions)
        {
            AssignmentData = new KafkaCoordinatorGroupAssignmentData(topicPartitions, DateTime.UtcNow);
        }

        public void SetOffsetsData([NotNull] IReadOnlyDictionary<string, KafkaCoordinatorGroupOffsetsDataTopic> topics)
        {
            OffsetsData = new KafkaCoordinatorGroupOffsetsData(topics);
        }

        public void ResetData()
        {
            ResetDataExceptSession();
            SessionData = null;            
        }

        public void ResetDataExceptSession()
        {
            SessionData = null;
            ProtocolData = null;
            LeaderData = null;
            AssignmentData = null;
            OffsetsData = null;
            TopicMetadataPartitionIds.Clear();
        }
    }
}