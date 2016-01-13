using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaClientTopic> Topics;

        [NotNull]
        public readonly KafkaConsumerGroupSettings Settings;

        public KafkaCoordinatorGroupStatus Status;

        public int GroupGenerationId;
        public string GroupProtocolName;
        public short GroupProtocolVersion;
        public string MemberId;
        public bool MemberIsLeader;
        [CanBeNull] public IReadOnlyList<KafkaCoordinatorGroupMember> GroupMembers;
        [CanBeNull] public IReadOnlyDictionary<string, List<KafkaCoordinatorGroupMember>> TopicMembers;

        [CanBeNull] public IReadOnlyList<string> AdditionalTopicNames;
        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicPartitions;

        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyList<int>> MemberAssignment;
        [CanBeNull] public IReadOnlyDictionary<string, IReadOnlyDictionary<int, long?>> TopicPartitionOffsets; //todo C004

        public DateTime HeartbeatTimestampUtc;
        public readonly TimeSpan HeartbeatPeriod;

        public KafkaCoordinatorGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Topics = topics;
            TopicPartitions = new Dictionary<string, IReadOnlyList<int>>();
            Settings = settings;
            var heartbeatPeriod = TimeSpan.FromSeconds(settings.GroupSessionTimeout.TotalSeconds/2 - 1);
            if (heartbeatPeriod < TimeSpan.FromMilliseconds(100))
            {
                heartbeatPeriod = TimeSpan.FromMilliseconds(100); //todo (E006)
            }
            HeartbeatPeriod = heartbeatPeriod;
        }

        [CanBeNull] public IReadOnlyDictionary<int, long?> GetPartitionOffsets([NotNull] string topicName)
        {
            if (Status != KafkaCoordinatorGroupStatus.Ready) return null;

            var topicPartitionOffsets = TopicPartitionOffsets;
            if (topicPartitionOffsets == null) return null;

            IReadOnlyDictionary<int, long?> partitionOffsets;
            topicPartitionOffsets.TryGetValue(topicName, out partitionOffsets);
            return partitionOffsets ?? new Dictionary<int, long?>();
        }
    }
}
