using JetBrains.Annotations;
using NKafka.Client.Broker.Diagnostics;

namespace NKafka.Client.Broker
{
    [PublicAPI]
    public interface IKafkaClientBrokerLogger
    {
        void OnBrokerConnected([NotNull] IKafkaClientBroker broker);

        void OnBrokerDisconnected([NotNull] IKafkaClientBroker broker);

        void OnBrokerError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaClientBrokerErrorInfo error);

        void OnBrokerRequestError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaClientBrokerErrorInfo error, 
            [NotNull] KafkaClientBrokerRequestInfo requestInfo);
    }
}
