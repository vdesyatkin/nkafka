using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public sealed class KafkaTopicMetadata
    {
        [NotNull]
        public readonly string TopicName;

        public readonly KafkaTopicMetadataError? Error;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaBrokerMetadata> Brokers;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaTopicPartitionMetadata> Partitions;

        public KafkaTopicMetadata([NotNull] string topicName, 
            KafkaTopicMetadataError? error,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaBrokerMetadata> brokers,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaTopicPartitionMetadata> partitions)
        {            
            TopicName = topicName;
            Error = error;
            Brokers = brokers;
            Partitions = partitions;
        }
    }
}
