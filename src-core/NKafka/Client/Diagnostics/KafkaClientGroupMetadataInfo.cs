using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{   
    [PublicAPI]
    public sealed class KafkaClientGroupMetadataInfo
    {
        [NotNull] public readonly string GroupName;        

        public readonly bool IsReady;

        public readonly KafkaClientGroupMetadataErrorCode? Error;

        [CanBeNull] public readonly KafkaGroupMetadata GroupMetadata;

        public readonly DateTime TimestampUtc;

        public KafkaClientGroupMetadataInfo([NotNull] string groupName, bool isReady, 
            KafkaClientGroupMetadataErrorCode? error, 
            [CanBeNull] KafkaGroupMetadata groupMetadata,
            DateTime timestampUtc)
        {
            GroupName = groupName;            
            IsReady = isReady;
            Error = error;
            GroupMetadata = groupMetadata;
            TimestampUtc = timestampUtc;
        }
    }
}
