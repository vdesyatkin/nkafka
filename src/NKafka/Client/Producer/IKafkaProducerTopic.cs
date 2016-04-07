using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerTopic
    {
        void EnqueueMessage([NotNull] KafkaMessage message);
        void EnqueueMessage([NotNull] byte[] key, [NotNull] byte[] data, DateTime timestampUtc);
        void EnqueueMessage([NotNull] byte[] data, DateTime timestampUtc);
    }

    [PublicAPI]
    public interface IKafkaProducerTopic<TKey, TData>
    {
        void Produce([NotNull] KafkaMessage<TKey, TData> message);
        void Produce([NotNull] TKey key, [NotNull] TData data, DateTime timestampUtc);
        void Produce([NotNull] TData data, DateTime timestampUtc);
    }
}
