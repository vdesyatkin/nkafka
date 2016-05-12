using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    internal interface IKafkaProducerTopicBufferLogger
    {
        void OnPartitioningError([NotNull] KafkaMessage message);
    }

    internal interface IKafkaProducerTopicBufferLogger<TKey, TData>
    {
        void OnPartitioningError([NotNull] KafkaMessage<TKey, TData> message);
        void OnSerializationError([NotNull] KafkaMessage<TKey, TData> message);
    }
}
