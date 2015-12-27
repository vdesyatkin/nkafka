using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientTopicPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitonId;
       
        [PublicAPI, NotNull]
        public readonly KafkaClientBrokerPartition BrokerPartition;        

        public KafkaClientTopicPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata,
            [CanBeNull] KafkaProducerTopicPartition producerPartition, [CanBeNull] KafkaConsumerTopicPartition consumerPartition)
        {
            TopicName = topicName;
            PartitonId = partitionId;
            BrokerPartition = new KafkaClientBrokerPartition(topicName, partitionId, brokerMetadata,
                producerPartition?.BrokerPartition, consumerPartition?.BrokerPartition);
        }
    }
}
