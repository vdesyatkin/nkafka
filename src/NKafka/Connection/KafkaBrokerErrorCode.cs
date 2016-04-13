using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerErrorCode : short
    {
        UnknownError = 0,   
        BadRequest = 1,
        InvalidState = 2,        
        TransportError = 3,
        ProtocolError = 4,
        Timeout = 5
    }
}
