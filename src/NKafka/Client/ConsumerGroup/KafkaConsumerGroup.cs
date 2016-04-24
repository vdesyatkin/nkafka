using System;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup
{
    internal sealed class KafkaConsumerGroup : IKafkaConsumerGroup
    {
        string IKafkaConsumerGroup.GroupName => GroupName;
        public readonly string GroupName;

        public readonly KafkaConsumerGroupSettings Settings;

        [CanBeNull]
        public KafkaClientGroup ClientGroup;

        public KafkaConsumerGroup(string groupName, KafkaConsumerGroupSettings settings)
        {
            GroupName = groupName;
            Settings = settings;
        }

        public KafkaConsumerGroupInfo GetDiagnosticsInfo()
        {
            var coordinator = ClientGroup?.Coordinator;
            var brokerMetadata = ClientGroup?.BrokerGroup?.BrokerMetadata;            

            if (coordinator == null)
            {
                return new KafkaConsumerGroupInfo(GroupName, 
                    DateTime.UtcNow, //todo first timstamp
                    false, 
                    KafkaConsumerGroupStatus.ToDo, //todo
                    KafkaConsumerGroupErrorCode.UnknownError, //todo
                    brokerMetadata, //todo brokerInfo
                    null, 
                    null,                    
                    null //todo emptyTopics
                    );
            }

            return new KafkaConsumerGroupInfo(GroupName, coordinator.GroupTimestampUtc,
                false, //todo 
                KafkaConsumerGroupStatus.ToDo, //todo 
                KafkaConsumerGroupErrorCode.UnknownError, //todo
                brokerMetadata, //todo brokerInfo
                new KafkaConsumerGroupProtocolInfo(coordinator.GroupProtocolName, coordinator.GroupProtocolVersion, coordinator.GroupAssignmentStrategyName, null), //todo
                new KafkaConsumerGroupMemberInfo(coordinator.GroupGenerationId, coordinator.MemberId, coordinator.MemberIsLeader),
                new KafkaConsumerGroupTopicInfo[0] //todo
                );            
        }
    }
}
