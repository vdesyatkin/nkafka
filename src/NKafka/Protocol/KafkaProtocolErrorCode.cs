namespace NKafka.Protocol
{
    internal enum KafkaProtocolErrorCode
    {
        UnknownError = 0,
        InvalidDataSize = 1,
        InvalidMessageSize = 2,
        InvalidItemCount = 3
    }
}
