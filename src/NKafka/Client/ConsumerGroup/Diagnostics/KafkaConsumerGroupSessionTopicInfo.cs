using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly IReadOnlyList<int> AssignedPartitionIds;

        public KafkaConsumerGroupSessionTopicInfo([NotNull]string topicName, [NotNull]IReadOnlyList<int> assignedPartitionIds)
        {
            TopicName = topicName;
            AssignedPartitionIds = assignedPartitionIds;
        }
    }
}
