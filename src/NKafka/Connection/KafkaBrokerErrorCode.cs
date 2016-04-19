using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerErrorCode
    {
        UnknownError = 0,
        Closed = 1,
        Maintenance = 2,
        BadRequest = 3,                
        TransportError = 4,
        ProtocolError = 5,
        Timeout = 6
    }
}
