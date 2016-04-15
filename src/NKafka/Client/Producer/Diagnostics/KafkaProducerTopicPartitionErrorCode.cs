using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{    
    [PublicAPI]
    public enum KafkaProducerTopicPartitionErrorCode : byte
    {
        UnknownError = 0
    }
}
