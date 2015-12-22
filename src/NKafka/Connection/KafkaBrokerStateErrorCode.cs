namespace NKafka.Connection
{
    internal enum KafkaBrokerStateErrorCode : short
    {
        Unknown = 0,
        ConnectionError = 1,
        IOError = 2,
        DataError = 3,
        Timeout = 4    
    }
}
