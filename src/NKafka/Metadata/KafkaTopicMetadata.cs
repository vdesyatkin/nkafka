using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Metadata
{
    internal sealed class KafkaTopicMetadata
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaBrokerMetadata> Brokers;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaTopicPartitionMetadata> Partitions;

        public KafkaTopicMetadata([NotNull] string topicName,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaBrokerMetadata> brokers,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaTopicPartitionMetadata> partitions)
        {
            TopicName = topicName;
            Brokers = brokers;
            Partitions = partitions;
        }
    }
}
