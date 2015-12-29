using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerTopic
    {
        void EnqueueMessage([NotNull] KafkaMessage message);
        void EnqueueMessage([NotNull] byte[] key, [NotNull] byte[] data);
        void EnqueueMessage([NotNull] byte[] data);
    }

    [PublicAPI]
    public interface IKafkaProducerTopic<TKey, TData>
    {
        void EnqueueMessage([NotNull] KafkaMessage<TKey, TData> message);
        void EnqueueMessage([NotNull] TKey key, [NotNull] TData data);
        void EnqueueMessage([NotNull] TData data);
    }
}
