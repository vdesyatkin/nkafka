using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public enum KafkaClientTopicMetadataErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        TransportError = 2,
        ProtocolError = 3,
        ClientTimeout = 4,
        ClientError = 5,
        MetadataError = 6,

        HostUnreachable = -7,
        HostNotAvailable = -8,
        NotAuthorized = -9
    }
}