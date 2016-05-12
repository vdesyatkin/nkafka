using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Broker
{
    internal sealed class KafkaClientBrokerGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        public bool IsUnplugRequired;

        public KafkaClientBrokerGroupStatus Status;

        [NotNull] public readonly KafkaCoordinatorGroup Coordinator;

        public KafkaClientBrokerGroup([NotNull] string groupName, [NotNull] KafkaBrokerMetadata brokerMetadata,            
            [NotNull] KafkaCoordinatorGroup cooridnator)
        {
            GroupName = groupName;
            BrokerMetadata = brokerMetadata;
            Coordinator = cooridnator;
        }        
    }
}
