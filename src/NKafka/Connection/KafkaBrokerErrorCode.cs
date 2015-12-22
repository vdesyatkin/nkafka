using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerErrorCode : short
    {
        Unknown = 0,      
        BadRequest = 1,
        DataError = 2,
        TransportError = 3,
        Timeout = 4
    }
}
