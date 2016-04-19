using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerFallbackHandler
    {
        void HandleMessageFallback(KafkaProducerFallbackInfo fallbackInfo);
    }

    [PublicAPI]
    public interface IKafkaProducerFallbackHandler<TKey, TData>
    {
        void HandleMessageFallback(KafkaProducerFallbackInfo<TKey, TData> fallbackInfo);
    }
}
