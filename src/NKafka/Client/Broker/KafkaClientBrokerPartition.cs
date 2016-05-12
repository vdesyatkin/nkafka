using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Broker
{
    internal sealed class KafkaClientBrokerPartition
    {
        [NotNull] public readonly string TopicName;
        
        public readonly int PartitionId;

        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        [CanBeNull] public readonly KafkaProducerBrokerPartition Producer;

        [CanBeNull] public readonly KafkaConsumerBrokerPartition Consumer;        
        
        public bool IsUnplugRequired;

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
