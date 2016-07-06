using System;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupInfo
    {
        [NotNull] public readonly string GroupName;        

        public readonly bool IsReady;        

        [CanBeNull] public readonly KafkaClientGroupMetadataInfo MetadataInfo;

        [CanBeNull] public readonly KafkaConsumerGroupSessionInfo SessionInfo;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupInfo([NotNull] string groupName, 
            bool isReady,
            [CanBeNull] KafkaClientGroupMetadataInfo metadataInfo,
            [CanBeNull] KafkaConsumerGroupSessionInfo sessionInfo,
            DateTime timestampUtc)
        {
            GroupName = groupName;
            IsReady = isReady;                  
            MetadataInfo = metadataInfo;
            SessionInfo = sessionInfo;
            TimestampUtc = timestampUtc;
        }
    }
}
