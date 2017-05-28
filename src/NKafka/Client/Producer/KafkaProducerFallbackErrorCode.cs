using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public enum KafkaProducerFallbackErrorCode
    {
        ClientStopped = 0,
        MessageSizeTooLarge = 1,
        MessageSizeLargerThanBatchMaxSize = 2,
        SerializationError = 3,
        PartitioningError = 4,
        PartitionNotFound = 5
    }
}