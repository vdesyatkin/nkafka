namespace NKafka.Connection
{    
    internal enum KafkaBrokerErrorCode
    {
        UnknownError = 0,
        ConnectionClosed = 1,
        ConnectionMaintenance = 2,
        BadRequest = 3,                
        TransportError = 4,
        ProtocolError = 5,
        ClientTimeout = 6,
        Cancelled = 7
    }
}
