using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{   
    [PublicAPI]
    public interface IKafkaConsumerFallbackHandler
    {
        void OnСommitFallback([NotNull] KafkaConsumerFallbackInfo fallbackInfo);
    }
}
