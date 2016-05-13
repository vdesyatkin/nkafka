using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    internal interface IKafkaConsumerGroupCoordinatorLogger
    {
        void OnTransportError([NotNull] KafkaConsumerGroupTransportErrorInfo error);

        void OnServerRebalance([NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolError([NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] KafkaConsumerGroupProtocolErrorInfo error);
    }
}
