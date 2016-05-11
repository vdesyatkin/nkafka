using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    internal enum KafkaProtocolErrorCode
    {
        UnknownError = 0,
        InvalidRequestType = 1,
        InvalidDataSize = 2,
        InvalidItemCount = 3,
        InvalidMessageSize = 4,
        InvalidMessageCrc = 5        
    }
}
