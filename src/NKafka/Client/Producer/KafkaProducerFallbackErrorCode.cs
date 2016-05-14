using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public enum KafkaProducerFallbackErrorCode
    {
        ClientStopped = 0,
        TooLargeSize = 1,        
    }
}
