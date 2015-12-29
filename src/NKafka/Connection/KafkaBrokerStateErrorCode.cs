using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerStateErrorCode : short
    {
        Unknown = 0,
        ConnectionError = 1,
        // ReSharper disable once InconsistentNaming
        IOError = 2,
        DataError = 3,
        Timeout = 4    
    }
}
