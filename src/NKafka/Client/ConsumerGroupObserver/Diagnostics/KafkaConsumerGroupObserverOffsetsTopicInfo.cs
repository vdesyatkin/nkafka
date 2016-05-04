using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverOffsetsTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupObserverOffsetsPartitionInfo> PartitionIds;

        public KafkaConsumerGroupObserverOffsetsTopicInfo([NotNull]string topicName, [NotNull]IReadOnlyList<KafkaConsumerGroupObserverOffsetsPartitionInfo> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
        }
    }
}
