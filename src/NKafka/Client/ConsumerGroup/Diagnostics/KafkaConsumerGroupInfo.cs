using System;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupInfo
    {
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        [NotNull]
        public readonly KafkaClientGroupMetadataInfo MetadataInfo;

        [NotNull]
        public readonly KafkaConsumerGroupSessionInfo SessionInfo;

        public KafkaConsumerGroupInfo(string groupName, DateTime timestampUtc, bool isReady,
            [NotNull] KafkaClientGroupMetadataInfo metadataInfo,
            [NotNull] KafkaConsumerGroupSessionInfo sessionInfo)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;            
            MetadataInfo = metadataInfo;            
        }
    }
}
