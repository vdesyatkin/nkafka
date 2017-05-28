using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public enum KafkaConsumerGroupType
    {
        /// <summary>
        /// Doesn't join the group, just use all partitions.
        /// </summary>        
        SingleConsumer = 0,

        /// <summary>
        /// Join the group and use part of partitions.
        /// </summary>
        BalancedConsumers = 1,

        /// <summary>
        /// Only regular scan commited offset of the group.
        /// </summary>
        Observer = 2,

        /// <summary>
        /// "In memory" group, allows to fetch without commit.
        /// </summary>
        Virtual = 3
    }
}