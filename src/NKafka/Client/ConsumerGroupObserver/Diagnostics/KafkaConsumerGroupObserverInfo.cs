using System;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverInfo
    {
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        [CanBeNull]
        public readonly KafkaClientGroupMetadataInfo MetadataInfo;

        [CanBeNull]
        public readonly KafkaConsumerGroupObserverSessionInfo SessionInfo;

        public KafkaConsumerGroupObserverInfo(string groupName, DateTime timestampUtc, bool isReady,
            [CanBeNull] KafkaClientGroupMetadataInfo metadataInfo,
            [CanBeNull] KafkaConsumerGroupObserverSessionInfo sessionInfo)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            MetadataInfo = metadataInfo;
        }
    }
}
