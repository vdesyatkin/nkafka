using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerFallbackHandler
    {
        void HandleMessageFallback([NotNull] KafkaProducerFallbackInfo fallbackInfo);
    }

    [PublicAPI]
    public interface IKafkaProducerFallbackHandler<TKey, TData>
    {
        void HandleMessageFallback([NotNull] KafkaProducerFallbackInfo<TKey, TData> fallbackInfo);
    }
}
