using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{   
    [PublicAPI]
    public interface IKafkaConsumerFallbackHandler
    {
        void HandleСommitFallback([NotNull] KafkaConsumerFallbackInfo fallbackInfo);
    }
}
