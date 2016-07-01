using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Connection.Logging;
using NKafka.Metadata;

namespace NKafka.Client
{
    [PublicAPI]
    public interface IKafkaClientLogger
    {
        void OnBrokerConnected([NotNull] IKafkaClientBroker broker);              

        void OnBrokerTransportError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerTransportErrorInfo error);

        void OnBrokerProtocolError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaBrokerProtocolErrorInfo error);

        void OnGroupMetadataError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaGroupMetadata groupMetadata);

        void OnTopicMetadataError([NotNull] IKafkaClientBroker broker, [NotNull] KafkaTopicMetadata topicMetadata);
    }
}
