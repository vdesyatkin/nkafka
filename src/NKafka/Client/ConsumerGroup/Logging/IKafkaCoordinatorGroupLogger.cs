using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Logging
{
    [PublicAPI]
    internal interface IKafkaCoordinatorGroupLogger
    {
        void OnTransportError([NotNull] KafkaConsumerGroupTransportErrorInfo error);

        void OnAssignmentError([NotNull] KafkaConsumerGroupAssignmentErrorInfo error);

        void OnServerRebalance([NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolError([NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnErrorReset();
    }
}
