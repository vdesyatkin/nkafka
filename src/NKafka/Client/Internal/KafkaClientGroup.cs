using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.Diagnostics;
using NKafka.Client.Internal.Broker;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup : IKafkaConsumerCoordinator
    {
        [NotNull] public readonly string GroupName;

        [NotNull, ItemNotNull] private readonly IReadOnlyList<KafkaClientTopic> _topics;

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;

        [NotNull] private readonly KafkaConsumerGroupSettings _settings;

        [NotNull]
        public KafkaClientGroupInfo DiagnosticsInfo => _diagnosticsInfo;
        [NotNull]
        private KafkaClientGroupInfo _diagnosticsInfo;

        public KafkaClientGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            _topics = topics;
            _settings = settings;
            _diagnosticsInfo = new KafkaClientGroupInfo(groupName, DateTime.UtcNow, false, null, null);
        }

        public void ChangeMetadataState(bool isReady, KafkaClientGroupErrorCode? errorCode, [CanBeNull] KafkaGroupMetadata metadata)
        {
            _diagnosticsInfo = new KafkaClientGroupInfo(GroupName, DateTime.UtcNow, isReady, errorCode, metadata);
            if (isReady && metadata?.Coordinator != null)
            {
                ApplyMetadata(metadata.Coordinator);
            }
        }

        private void ApplyMetadata([NotNull] KafkaBrokerMetadata coordinatorBroker)
        {            
            var coordinator = new KafkaCoordinatorGroup(GroupName, _topics, _settings);            
            var brokerGroup = new KafkaClientBrokerGroup(GroupName, coordinatorBroker, coordinator);            
            BrokerGroup = brokerGroup;
        }

        public IReadOnlyDictionary<int, long?> GetPartitionOffsets(string topicName)
        {
            return BrokerGroup?.Coordinator.GetPartitionOffsets(topicName);
        }
    }
}
