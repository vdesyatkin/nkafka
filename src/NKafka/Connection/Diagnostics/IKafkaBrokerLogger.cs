using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    internal interface IKafkaBrokerLogger
    {
        void OnConnected();

        void OnDisconnected();        

        void OnConnectionError([NotNull] KafkaBrokerConnectionErrorInfo error);

        void OnProtocolError([NotNull] KafkaBrokerProtocolErrorInfo error);        
    }
}
