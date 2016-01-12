using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal.Broker;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull]
        public IReadOnlyList<KafkaClientTopic> Topics { get; private set; }

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;

        public KafkaClientGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics)
        {
            GroupName = groupName;
            Topics = topics;
        }

        public void ApplyCoordinator([NotNull] KafkaBrokerMetadata groupCoordinator)
        {
            var brokerGroup = new KafkaClientBrokerGroup(GroupName, groupCoordinator);
            brokerGroup.SetTopics(Topics);
            BrokerGroup = brokerGroup;
        }
    }
}
