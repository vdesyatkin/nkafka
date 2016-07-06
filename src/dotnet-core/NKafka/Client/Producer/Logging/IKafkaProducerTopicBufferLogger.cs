using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    internal interface IKafkaProducerTopicBufferLogger
    {
        void OnPartitioningError([NotNull] KafkaProducerTopicPartitioningErrorInfo error);
    }

    internal interface IKafkaProducerTopicBufferLogger<TKey, TData>
    {
        void OnPartitioningError([NotNull] KafkaProducerTopicPartitioningErrorInfo<TKey, TData> error);
        void OnSerializationError([NotNull] KafkaProducerTopicSerializationErrorInfo<TKey, TData> error);
    }
}
