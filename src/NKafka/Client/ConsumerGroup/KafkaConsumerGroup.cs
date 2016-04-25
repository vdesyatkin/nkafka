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
            var clientGroup = ClientGroup;
            if (clientGroup == null)
            {
                return new KafkaConsumerGroupInfo(GroupName, DateTime.UtcNow, false, null, null);
            }
            var metadataInfo = clientGroup.MetadataInfo;
            var sessionInfo = clientGroup.Coordinator.GetSessionInfo();

            return new KafkaConsumerGroupInfo(GroupName, DateTime.UtcNow,
                metadataInfo.IsReady && sessionInfo.IsReady,
                metadataInfo,
                sessionInfo
                );
        }
    }
}
