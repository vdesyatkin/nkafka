using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Broker.Internal;
using NKafka.Client.ConsumerGroup;
using NKafka.Client.ConsumerGroup.Internal;
using NKafka.Client.ConsumerGroup.Logging;
using NKafka.Client.Diagnostics;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {        
        [NotNull] public readonly string GroupName;

        [NotNull] public readonly KafkaCoordinatorGroup Coordinator;        

        public KafkaClientGroupStatus Status;

        [CanBeNull] public KafkaClientBrokerGroup BrokerGroup;        

        [NotNull] public KafkaClientGroupMetadataInfo MetadataInfo { get; private set; }

        public KafkaClientGroup([NotNull] string groupName, KafkaConsumerGroupType groupType, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaClientTopic> topics, 
            [NotNull] KafkaConsumerGroupSettings settings,
            [CanBeNull] IKafkaCoordinatorGroupLogger logger)
        {
            GroupName = groupName;
            var groupCoordinatorName = $"group[{groupName}]";
            Coordinator = new KafkaCoordinatorGroup(groupName, groupCoordinatorName, groupType, topics, settings, logger);
            MetadataInfo = new KafkaClientGroupMetadataInfo(groupName, false, null, null, DateTime.UtcNow);
        }

        public void ChangeMetadataState(bool isReady, KafkaClientGroupMetadataErrorCode? errorCode, [CanBeNull] KafkaGroupMetadata metadata)
        {
            MetadataInfo = new KafkaClientGroupMetadataInfo(GroupName, isReady, errorCode, metadata, DateTime.UtcNow);
            Coordinator.GroupMetadataInfo = MetadataInfo;
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
    }
}
