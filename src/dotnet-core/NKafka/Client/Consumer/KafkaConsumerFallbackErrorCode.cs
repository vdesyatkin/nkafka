using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{    
    [PublicAPI]
    public enum KafkaConsumerFallbackErrorCode
    {
        ClientStopped = 0,
        UnassignedBeforeCommit = 1
    }
}
