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
        UnexpectedResponseSize = 3,
        InvalidDataSize = 4,
        UnexpectedDataSize = 5,
        InvalidStringSize = 6,
        InvalidItemCount = 7,
        InvalidMessageSize = 8,
        UnexpectedMessageSize = 9,
        InvalidMessageCrc = 10        
    }
}
