using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition
    {        
        public readonly int PartitonId;       

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;       
                

        public KafkaProducerTopicPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaProducerSettings settings, [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler)
        {            
            PartitonId = partitionId;            
            BrokerPartition = new KafkaProducerBrokerPartition(topicName, partitionId, settings, fallbackHandler);
        }       
    }
}
