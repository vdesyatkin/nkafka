namespace NKafka.Connection
{    
    internal enum KafkaConnectionErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        ConnectionMaintenance = 2,
        BadRequest = 3,
        TransportError = 4,        
        ClientTimeout = 6,
        Cancelled = 7,

        InvalidHost = 8,
        UnsupportedHost = 9,
        NetworkNotAvailable = 10,
        HostNotAvailable,
        NotAuthorized,
        UnsupportedOperation = 9,
        TooBigMessage
    }
}
