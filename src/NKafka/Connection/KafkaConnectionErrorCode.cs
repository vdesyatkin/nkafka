namespace NKafka.Connection
{    
    internal enum KafkaConnectionErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        ConnectionMaintenance = 2,
        BadRequest = 3,
        TransportError = 4,        
        ClientTimeout = 5,
        Cancelled = 6,

        InvalidHost = 7,
        UnsupportedHost = 8,
        NetworkNotAvailable = 9,
        ConnectionNotAllowed = 10,
        ConnectionRefused = 11,
        HostUnreachable = 12,
        HostNotAvailable = 13,
        NotAuthorized = 14,

        UnsupportedOperation = 15,
        OperationRefused = 16,
        TooBigMessage = 17
    }
}
