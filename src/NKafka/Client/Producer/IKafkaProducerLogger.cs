using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerLogger
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic topic, KafkaMessage message);
    }

    [PublicAPI]
    public interface IKafkaProducerLogger<TKey, TData>
    {
        void OnPartitioningError([NotNull] IKafkaProducerTopic<TKey, TData> topic, KafkaMessage<TKey, TData> message);
        void OnSerializationError([NotNull] IKafkaProducerTopic<TKey, TData> topic, KafkaMessage<TKey, TData> message);        
    }
}
