using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public enum KafkaClientTopicMetadataErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        ClientMaintenance = 2,
        TransportError = 3,
        ProtocolError = 4,
        ClientTimeout = 5,
        ClientError = 6,
        MetadataError = 7,

        HostUnreachable = -7,
        HostNotAvailable = -8,
        NotAuthorized = -9
    }
}
