using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{    
    [PublicAPI]
    public enum KafkaBrokerErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        ConnectionMaintenance = 2,
        BadRequest = 3,                
        TransportError = 4,
        ProtocolError = 5,
        ClientTimeout = 6,
        Cancelled = 7,     

        ConnectionRefused = 8,
        HostUnreachable = 9,
        HostNotAvailable = 10,
        NotAuthorized = 11,        
        OperationRefused = 12,
        TooBigMessage = 13
    }
}
