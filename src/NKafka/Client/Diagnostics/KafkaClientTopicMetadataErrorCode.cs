using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public enum KafkaClientTopicMetadataErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        Maintenance = 2,
        TransportError = 3,
        ProtocolError = 4,
        Timeout = 5,
        InternalError = 6,
        MetadataError = 7,        
    }
}
