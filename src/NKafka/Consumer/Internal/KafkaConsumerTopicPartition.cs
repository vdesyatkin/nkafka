using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Consumer.Internal
{
    class KafkaConsumerTopicPartition
    {
        public readonly string TopicName;

        public readonly int PartitonId;
        
        [NotNull]
        public readonly KafkaConsumerBrokerPartition BrokerPartition;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata, [NotNull] IKafkaConsumerTopic dataConsumer)
        {
            TopicName = topicName;
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, partitionId, brokerMetadata, dataConsumer);
        }        
    }
}
