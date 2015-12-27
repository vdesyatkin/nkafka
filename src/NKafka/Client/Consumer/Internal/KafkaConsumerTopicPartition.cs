using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitonId;

        [PublicAPI, NotNull]
        public readonly KafkaConsumerBrokerPartition BrokerPartition;        

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId, [NotNull] IKafkaConsumerTopic consumer)
        {
            TopicName = topicName;
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(TopicName, PartitonId, consumer);
        }        
    }
}
