using JetBrains.Annotations;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerLogger
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicPartitioningErrorInfo error);

        void OnTransportError([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnPartitionErrorReset([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicErrorResetInfo partitionError);
    }

    [PublicAPI]
    public interface IKafkaProducerLogger<TKey, TData>
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicPartitioningErrorInfo<TKey, TData> error);

        void OnSerializationError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicSerializationErrorInfo<TKey, TData> error);

        void OnTransportError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnPartitionErrorReset([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicErrorResetInfo partitionError);
    }
}
