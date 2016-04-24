using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{   
    [PublicAPI]
    public sealed class KafkaClientGroupMetadataInfo
    {
        [NotNull]
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaClientGroupMetadataErrorCode? Error;

        [CanBeNull]
        public readonly KafkaGroupMetadata GroupMetadata;

        public KafkaClientGroupMetadataInfo([NotNull] string groupName, DateTime timestampUtc, bool isReady, KafkaClientGroupMetadataErrorCode? error, [CanBeNull] KafkaGroupMetadata groupMetadata)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Error = error;
            GroupMetadata = groupMetadata;
        }
    }
}
