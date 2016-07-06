using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupOffsetsTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupOffsetsPartitionInfo> AssignedPartitionIds;

        public KafkaConsumerGroupOffsetsTopicInfo([NotNull]string topicName, [NotNull]IReadOnlyList<KafkaConsumerGroupOffsetsPartitionInfo> assignedPartitionIds)
        {
            TopicName = topicName;
            AssignedPartitionIds = assignedPartitionIds;
        }
    }
}
