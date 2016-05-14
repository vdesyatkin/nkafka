using JetBrains.Annotations;
using NKafka.Connection.Logging;

namespace NKafka.Client.Broker.Diagnostics
{
    [PublicAPI]
    public interface IKafkaClientBrokerLogger
    {
        void OnBrokerConnected([NotNull] IKafkaClientBroker broker);              

        void OnBrokerConnectionError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerConnectionErrorInfo error);

        void OnBrokerProtocolError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerProtocolErrorInfo error);        
    }
}
