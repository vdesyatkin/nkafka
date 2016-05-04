using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{    
    [PublicAPI]
    public enum KafkaConsumerGroupObserverStatus
    {
        NotInitialized = 0,
        Rearrange = 1,        
        OffsetsFilling = 2,
        Ready = 3,
        Error = 4,        
    }
}
