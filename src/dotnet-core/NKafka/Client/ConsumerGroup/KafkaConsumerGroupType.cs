using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public enum KafkaConsumerGroupType
    {
        SingleConsumer = 0,
        BalancedConsumers = 1,
        Observer = 2
    }
}
