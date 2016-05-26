using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerFallbackHandler
    {
        void OnMessageFallback([NotNull] KafkaProducerFallbackInfo fallbackInfo);
    }

    [PublicAPI]
    public interface IKafkaProducerFallbackHandler<TKey, TData>
    {
        void OnMessageFallback([NotNull] KafkaProducerFallbackInfo<TKey, TData> fallbackInfo);
    }
}
