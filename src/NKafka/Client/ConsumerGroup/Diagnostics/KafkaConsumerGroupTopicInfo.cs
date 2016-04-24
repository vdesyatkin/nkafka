using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupTopicInfo
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<int> AssignedPartitionIds;

        public KafkaConsumerGroupTopicInfo(string topicName, IReadOnlyList<int> assignedPartitionIds)
        {
            TopicName = topicName;
            AssignedPartitionIds = assignedPartitionIds;
        }
    }
}
