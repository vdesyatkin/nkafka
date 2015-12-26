using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerErrorCode : short
    {
        Unknown = 0,   
        BadRequest = 1,
        InvalidState = 2,
        DataError = 3,
        TransportError = 4,
        Timeout = 5
    }
}
