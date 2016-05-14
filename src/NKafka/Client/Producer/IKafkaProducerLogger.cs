using JetBrains.Annotations;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerLogger
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic topic, KafkaProducerTopicPartitioningErrorInfo error);

        void OnTransportError([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaProducerTopic topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnErrorReset([NotNull] IKafkaProducerTopic topic);
    }

    [PublicAPI]
    public interface IKafkaProducerLogger<TKey, TData>
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic<TKey, TData> topic, KafkaProducerTopicPartitioningErrorInfo<TKey, TData> error);

        void OnSerializationError([NotNull] IKafkaProducerTopic<TKey, TData> topic, KafkaProducerTopicSerializationErrorInfo<TKey, TData> error);

        void OnTransportError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicTransportErrorInfo error);

        void OnServerRebalance([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolError([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnProtocolWarning([NotNull] IKafkaProducerTopic<TKey, TData> topic, [NotNull] KafkaProducerTopicProtocolErrorInfo error);

        void OnErrorReset([NotNull] IKafkaProducerTopic<TKey, TData> topic);
    }
}
