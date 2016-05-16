using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public enum KafkaProducerFallbackErrorCode
    {
        ClientStopped = 0,
        MessageSizeTooLarge = 1,
        MessageSizeLargerThanBatchMaxSize = 2
    }
}
