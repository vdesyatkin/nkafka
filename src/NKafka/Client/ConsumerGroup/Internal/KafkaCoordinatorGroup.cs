using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] public readonly IReadOnlyDictionary<string, KafkaClientTopic> Topics;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;

        public KafkaCoordinatorGroupStatus Status;

        public KafkaConsumerGroupSessionErrorCode? Error { get; private set; }
        public DateTime ErrorTimestampUtc { get; private set; }

        public int GroupGenerationId;
        public string GroupProtocolName;
        public string GroupAssignmentStrategyName;
        public short GroupProtocolVersion;
        public string MemberId;
        public bool MemberIsLeader;
        [CanBeNull] public IReadOnlyList<KafkaCoordinatorGroupMember> GroupMembers;
        [CanBeNull] public IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;

        [CanBeNull] public IReadOnlyList<string> AdditionalTopicNames;
        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> AllTopicPartitions;

        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyList<int>> AssignedTopicPartitions;
        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyDictionary<int, long?>> AssignedTopicPartitionOffsets;
        [NotNull] public readonly Dictionary<string, Dictionary<int, long?>> CommitedTopicPartitionOffsets;

        public DateTime HeartbeatTimestampUtc;
        public readonly TimeSpan HeartbeatPeriod;

        public DateTime CommitTimestampUtc;
        public readonly TimeSpan CommitPeriod;

        public KafkaCoordinatorGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;            
            AllTopicPartitions = new Dictionary<string, IReadOnlyList<int>>();
            CommitedTopicPartitionOffsets = new Dictionary<string, Dictionary<int, long?>>();
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

        [CanBeNull] public IReadOnlyDictionary<int, long?> GetPartitionOffsets([NotNull] string topicName)
        {
            if (Status != KafkaCoordinatorGroupStatus.Ready) return null;

            var topicPartitionOffsets = AssignedTopicPartitionOffsets;
            if (topicPartitionOffsets == null) return null;

            IReadOnlyDictionary<int, long?> partitionOffsets;
            topicPartitionOffsets.TryGetValue(topicName, out partitionOffsets);
            return partitionOffsets ?? new Dictionary<int, long?>();
        }

        [NotNull]
        public KafkaConsumerGroupSessionInfo GetSessionInfo()
        {
            return new KafkaConsumerGroupSessionInfo(GroupName, ErrorTimestampUtc,
                false, //todo 
                KafkaConsumerGroupSessionStatus.ToDo, //todo 
                KafkaConsumerGroupSessionErrorCode.UnknownError, //todo                
                new KafkaConsumerGroupSessionTopicInfo[0], //todo
                new KafkaConsumerGroupSessionProtocolInfo(GroupProtocolName, GroupProtocolVersion, GroupAssignmentStrategyName, null), //todo
                new KafkaConsumerGroupSessionMemberInfo(GroupGenerationId, MemberId, MemberIsLeader)              
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
    }
}