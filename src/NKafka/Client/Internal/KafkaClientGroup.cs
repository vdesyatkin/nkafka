using JetBrains.Annotations;
using NKafka.Client.Internal.Broker;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {
        [NotNull] public readonly string GroupName;

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;

        public KafkaClientGroup([NotNull] string groupName)
        {
            GroupName = groupName;
        }

        public void ApplyCoordinator([NotNull] KafkaBrokerMetadata groupCoordinator)
        {
            BrokerGroup = new KafkaClientBrokerGroup(GroupName, groupCoordinator);
        }
    }
}
