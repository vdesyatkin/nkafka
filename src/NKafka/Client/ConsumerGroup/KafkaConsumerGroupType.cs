using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public enum KafkaConsumerGroupType
    {
        SingleConsumer,
        BalancedConsumers
    }
}
