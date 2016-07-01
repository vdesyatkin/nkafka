using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public interface IKafkaSerializer<TKey, TData>
    {
        KafkaMessage SerializeMessage([NotNull] KafkaMessage<TKey, TData> message);
        KafkaMessage<TKey, TData> DeserializeMessage([NotNull] KafkaMessage message);
    }
}
