using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;

namespace NKafka.Client.Broker.Diagnostics
{
    [PublicAPI]
    public interface IKafkaClientBrokerLogger
    {
        void OnBrokerConnected([NotNull] IKafkaClientBroker broker);

        void OnBrokerDisconnected([NotNull] IKafkaClientBroker broker);

        void OnBrokerError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerErrorInfo error);

        void OnBrokerConnectionError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerConnectionErrorInfo error);

        void OnBrokerProtocolError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerProtocolErrorInfo error);        
    }
}
