using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    public enum KafkaProtocolErrorCode
    {
        UnknownError = 0,
        // ReSharper disable once InconsistentNaming
        IOError = 1,
        InvalidRequestType = 2,
        InvalidDataSize = 3,
        InvalidStringSize = 4,
        InvalidItemCount = 5,
        InvalidMessageSize = 6,
        InvalidMessageCrc = 7        
    }
}
