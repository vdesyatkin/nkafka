using System;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.ConsumerGroupObserver.Diagnostics;
using NKafka.Client.Internal;

namespace NKafka.Client.ConsumerGroupObserver
{
    internal sealed class KafkaConsumerGroupObserver : IKafkaConsumerGroupObserver
    {
        string IKafkaConsumerGroupObserver.GroupName => GroupName;        

        [NotNull] public readonly string GroupName;        

        [NotNull] public readonly KafkaConsumerGroupObserverSettings Settings;

        [CanBeNull] public KafkaClientGroup ClientGroup;

        public KafkaConsumerGroupObserver([NotNull] string groupName, [NotNull] KafkaConsumerGroupObserverSettings settings)
        {
            GroupName = groupName;            
            Settings = settings;
        }

        public KafkaConsumerGroupObserverInfo GetDiagnosticsInfo()
        {
            var clientGroup = ClientGroup;
            if (clientGroup == null)
            {
                return new KafkaConsumerGroupObserverInfo(GroupName, DateTime.UtcNow, false, null, null);
            }
            //todo(E012)
            //var metadataInfo = clientGroup.MetadataInfo;
            //var sessionInfo = clientGroup.Coordinator.GetSessionDiagnosticsInfo();

            //return new KafkaConsumerGroupObserverInfo(GroupName, DateTime.UtcNow,
            //    metadataInfo.IsReady && sessionInfo.IsReady,
            //    metadataInfo,
            //    sessionInfo);

            return new KafkaConsumerGroupObserverInfo(GroupName, DateTime.UtcNow, false, null, null);
        }
    }
}
