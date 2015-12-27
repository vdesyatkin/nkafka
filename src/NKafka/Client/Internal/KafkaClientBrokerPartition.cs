using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientBrokerPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitionId;

        [PublicAPI, NotNull]
        public readonly KafkaBrokerMetadata BrokerMetadata;

        [PublicAPI, CanBeNull]
        public readonly KafkaProducerBrokerPartition Producer;

        [PublicAPI, CanBeNull]
        public readonly KafkaConsumerBrokerPartition Consumer;

        [PublicAPI]
        public bool IsUnplugRequired;

        [PublicAPI]
        public KafkaClientBrokerPartitionStatus Status;   

        public KafkaClientBrokerPartition([NotNull]string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata,
            [CanBeNull] KafkaProducerBrokerPartition producerPartition, [CanBeNull] KafkaConsumerBrokerPartition consumerPartition)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            BrokerMetadata = brokerMetadata;
            Producer = producerPartition;
            Consumer = consumerPartition;
        }
    }
}
