namespace NKafka.Connection
{
    internal enum KafkaBrokerErrorCode : short
    {
        ConnectionError = 0,
        Timeout = 1,
        IOError = 2,
        DataError = 3,
    }
}
