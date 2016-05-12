using JetBrains.Annotations;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition
    {        
        public readonly int PartitonId;       

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;       
                
        public KafkaProducerTopicPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaProducerSettings settings, 
            [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler,
            [CanBeNull] IKafkaProducerTopicLogger logger)
        {            
            PartitonId = partitionId;            
            BrokerPartition = new KafkaProducerBrokerPartition(topicName, partitionId, settings, fallbackHandler, logger);
        }       
    }
}
