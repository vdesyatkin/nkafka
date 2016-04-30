using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{    
    [PublicAPI]
    public enum KafkaProducerBrokerPartitionStatus
    {
        NotInitialized = 0,
        RearrangeRequired = 1,
        Ready = 2,        
        Error = 3
    }
}
