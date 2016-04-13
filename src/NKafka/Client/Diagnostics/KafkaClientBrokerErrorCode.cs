using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public enum KafkaClientBrokerErrorCode
    {
        UnknownError = 0,
        ConnectionError = 1,
        TransportError = 2,
        ProtocolError = 3,
        Timeout = 4
    }
}
