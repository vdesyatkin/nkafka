using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionOffsetsTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupSessionOffsetsPartitionInfo> AssignedPartitionIds;

        public KafkaConsumerGroupSessionOffsetsTopicInfo([NotNull]string topicName, [NotNull]IReadOnlyList<KafkaConsumerGroupSessionOffsetsPartitionInfo> assignedPartitionIds)
        {
            TopicName = topicName;
            AssignedPartitionIds = assignedPartitionIds;
        }
    }
}
