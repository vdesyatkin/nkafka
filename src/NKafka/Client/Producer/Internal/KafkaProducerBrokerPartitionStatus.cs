using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{    
    [PublicAPI]
    public enum KafkaProducerBrokerPartitionStatus
    {
        NotInitialized = 0,        
        Plugged = 1,
        RearrangeRequired = 2
    }
}
