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
            var sessionInfo = ClientGroup?.Coordinator.GetSessionInfo();
            var metadataInfo = ClientGroup?.MetadataInfo;

            return new KafkaConsumerGroupInfo(GroupName, DateTime.UtcNow,
                false, //todo 
                metadataInfo, //todo empty value
                sessionInfo //todo empty value
                );   
        }
    }
}
