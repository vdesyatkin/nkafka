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

        void OnBrokerRequestError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerErrorInfo error, 
            [NotNull] KafkaBrokerRequestInfo requestInfo);
    }
}
