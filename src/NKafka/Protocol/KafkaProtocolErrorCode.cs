using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    public enum KafkaProtocolErrorCode
    {
        UnknownError = 0,
        InvalidRequestType = 1,
        InvalidDataSize = 2,
        InvalidStringSize = 3,
        InvalidItemCount = 4,
        InvalidMessageSize = 5,
        InvalidMessageCrc = 6        
    }
}
