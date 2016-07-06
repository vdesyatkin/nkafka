using JetBrains.Annotations;
using NKafka.Connection.Logging;

namespace NKafka.Connection
{
    [PublicAPI]
    public interface IKafkaBrokerLogger
    {
        void OnConnected();        

        void OnTransportError([NotNull] KafkaBrokerTransportErrorInfo error);

        void OnProtocolError([NotNull] KafkaBrokerProtocolErrorInfo error);        
    }
}
