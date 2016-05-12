using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientTopicPartition
    {
        public readonly int PartitionId;

        [NotNull] public readonly KafkaClientBrokerPartition BrokerPartition;

        [CanBeNull] public readonly KafkaProducerTopicPartition ProducerPartition;

        [CanBeNull] public readonly KafkaConsumerTopicPartition ConsumerPartition;

        public KafkaClientTopicPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata,
            [CanBeNull] KafkaProducerTopicPartition producerPartition, [CanBeNull] KafkaConsumerTopicPartition consumerPartition)
        {
            PartitionId = partitionId;
            ProducerPartition = producerPartition;
            ConsumerPartition = consumerPartition;
            BrokerPartition = new KafkaClientBrokerPartition(topicName, partitionId, brokerMetadata,
                producerPartition?.BrokerPartition, consumerPartition?.BrokerPartition);
        }
    }
}
