using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Internal.Broker;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientTopicPartition
    {                        
        [NotNull] public readonly KafkaClientBrokerPartition BrokerPartition;        

        public KafkaClientTopicPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata,
            [CanBeNull] KafkaProducerTopicPartition producerPartition, [CanBeNull] KafkaConsumerTopicPartition consumerPartition)
        {         
            BrokerPartition = new KafkaClientBrokerPartition(topicName, partitionId, brokerMetadata,
                producerPartition?.BrokerPartition, consumerPartition?.BrokerPartition);
        }
    }
}
