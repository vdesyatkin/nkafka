using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public enum KafkaProducerTopicErrorCode : byte
    {
        UnknownError = 0
    }
}
