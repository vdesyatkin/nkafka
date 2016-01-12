using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroups;
using NKafka.Client.ConsumerGroups.Internal;
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

        [NotNull] private readonly KafkaConsumerGroupSettings _settings;

        public KafkaClientGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Topics = new KafkaClientTopic[0];
            _settings = settings;
        }

        public void ApplyCoordinator([NotNull] KafkaBrokerMetadata coordinatorBroker)
        {
            var brokerGroup = new KafkaClientBrokerGroup(GroupName, coordinatorBroker, new KafkaCoordinatorGroup(_settings));
            brokerGroup.SetTopics(Topics);
            BrokerGroup = brokerGroup;
        }
    }
}
