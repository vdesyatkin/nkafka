using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public enum KafkaConsumerGroupSessionErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        ClientMaintenance = -2,
        ServerMaintenance = -3,
        TransportError = -4,
        ProtocolError = -5,
        ClientTimeout = -6,
    }
}
