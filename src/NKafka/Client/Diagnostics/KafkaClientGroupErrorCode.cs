using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{    
    [PublicAPI]
    public enum KafkaClientGroupErrorCode : byte
    {
        UnknownError = 0,
        InvalidState = 1,
        TransportError = 2,
        ProtocolError = 3,
        MetadataError = 4,
        InternalError = 5,
    }
}
