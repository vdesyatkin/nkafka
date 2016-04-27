﻿using System;
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

        [NotNull] public readonly KafkaCoordinatorGroup Coordinator;

        [NotNull, ItemNotNull] private readonly IReadOnlyList<KafkaClientTopic> _topics;

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;

        [NotNull] private readonly KafkaConsumerGroupSettings _settings;

        [NotNull] public KafkaClientGroupMetadataInfo MetadataInfo => _metadataInfo;
        [NotNull] private KafkaClientGroupMetadataInfo _metadataInfo;

        public KafkaClientGroup([NotNull] string groupName, [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, [NotNull] KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            _topics = topics;
            _settings = settings;
            Coordinator = new KafkaCoordinatorGroup(GroupName, _topics, _settings);
            _metadataInfo = new KafkaClientGroupMetadataInfo(groupName, DateTime.UtcNow, false, null, null);
        }

        public void ChangeMetadataState(bool isReady, KafkaClientGroupMetadataErrorCode? errorCode, [CanBeNull] KafkaGroupMetadata metadata)
        {
            _metadataInfo = new KafkaClientGroupMetadataInfo(GroupName, DateTime.UtcNow, isReady, errorCode, metadata);
            if (isReady && metadata?.Coordinator != null)
            {
                ApplyMetadata(metadata.Coordinator);
            }
        }

        private void ApplyMetadata([NotNull] KafkaBrokerMetadata coordinatorBroker)
        {            
            var brokerGroup = new KafkaClientBrokerGroup(GroupName, coordinatorBroker, Coordinator);            
            BrokerGroup = brokerGroup;
        }

        public IReadOnlyDictionary<int, IKafkaConsumerCoordinatorOffsetsData> GetPartitionOffsets(string topicName)
        {
            return BrokerGroup?.Coordinator.GetPartitionOffsets(topicName);
        }
    }
}
