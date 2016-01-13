using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.Internal.Broker;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] private readonly IReadOnlyList<KafkaClientTopic> _topics;

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;

        [NotNull] private readonly KafkaConsumerGroupSettings _settings;

        public KafkaClientGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            _topics = topics;
            _settings = settings;
        }

        public void ApplyCoordinator([NotNull] KafkaBrokerMetadata coordinatorBroker)
        {
            var coordinator = new KafkaCoordinatorGroup(GroupName, _topics, _settings);
            var brokerGroup = new KafkaClientBrokerGroup(GroupName, coordinatorBroker, coordinator);            
            BrokerGroup = brokerGroup;
        }
    }
}
