using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] public readonly IReadOnlyDictionary<string, KafkaClientTopic> Topics;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerGroupProtocolInfo> Protocols;

        public KafkaCoordinatorGroupStatus Status;

        public int GroupGenerationId;
        public string GroupProtocolName;
        public short GroupProtocolVersion;
        public string MemberId;
        public bool MemberIsLeader;
        [CanBeNull] public IReadOnlyList<KafkaCoordinatorGroupMember> GroupMembers;
        [CanBeNull] public IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;

        [CanBeNull] public IReadOnlyList<string> AdditionalTopicNames;
        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> AllTopicPartitions;

        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyList<int>> AssignedTopicPartitions;
        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyDictionary<int, long?>> AssignedTopicPartitionOffsets;
        [NotNull] public Dictionary<string, Dictionary<int, long?>> CommitedTopicPartitionOffsets;

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

            var heartbeatPeriod = TimeSpan.FromSeconds(settings.GroupSessionTimeout.TotalSeconds/2 - 1);
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

            var protocols = new List<KafkaConsumerGroupProtocolInfo>();
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

                    var protocol = new KafkaConsumerGroupProtocolInfo(settingsProtocol.ProtocolName,
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
    }
}
