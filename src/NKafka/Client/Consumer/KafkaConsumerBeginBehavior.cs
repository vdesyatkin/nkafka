using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public enum KafkaConsumerBeginBehavior
    {
        BeginFromMinAvailableOffset = 0,
        BeginAfterMaxAvailableOffset = 1
    }
}
