using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    internal enum KafkaBrokerStateErrorCode : short
    {
        UnknownError = 0,
        ConnectionError = 1,        
        TransportError = 2,
        ProtocolError = 3,
        Timeout = 4    
    }
}
