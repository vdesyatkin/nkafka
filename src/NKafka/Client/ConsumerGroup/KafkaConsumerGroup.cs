using System;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroup
{
    internal sealed class KafkaConsumerGroup : IKafkaConsumerGroup
    {
        string IKafkaConsumerGroup.GroupName => GroupName;
        KafkaConsumerGroupType IKafkaConsumerGroup.GroupType => GroupType;
               
        [NotNull] public readonly string GroupName;
        public readonly KafkaConsumerGroupType GroupType;
        public bool IsReady => ClientGroup?.Coordinator.IsReady == true;

        [NotNull] public readonly KafkaConsumerGroupSettings Settings;
        [CanBeNull] public readonly IKafkaConsumerGroupLogger Logger;

        [CanBeNull] public KafkaClientGroup ClientGroup;

        public KafkaConsumerGroup([NotNull] string groupName, KafkaConsumerGroupType groupType, 
            [NotNull] KafkaConsumerGroupSettings settings,
            [CanBeNull] IKafkaConsumerGroupLogger logger)
        {
            GroupName = groupName;
            GroupType = groupType;
            Settings = settings;
            Logger = logger;
        }

        public KafkaConsumerGroupInfo GetDiagnosticsInfo()
        {
            var clientGroup = ClientGroup;
            if (clientGroup == null)
            {
                return new KafkaConsumerGroupInfo(GroupName, false, null, null, DateTime.UtcNow);
            }

            return clientGroup.Coordinator.GetDiagnosticsInfo();
        }
    }
}
