using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Logging;

namespace NKafka.Client.ConsumerGroup
{    
    [PublicAPI]
    public interface IKafkaConsumerGroupLogger
    {
        void OnTransportError([NotNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerGroupTransportErrorInfo error);

        void OnAssignmentError([NotNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerGroupAssignmentErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerGroupProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaConsumerGroup group, [NotNull] KafkaConsumerGroupProtocolErrorInfo error);
    }
}
