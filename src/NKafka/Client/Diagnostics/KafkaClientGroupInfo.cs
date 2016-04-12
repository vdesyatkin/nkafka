using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{   
    [PublicAPI]
    public sealed class KafkaClientGroupInfo
    {
        [NotNull]
        public readonly string GroupName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaClientGroupErrorCode? Error;

        [CanBeNull]
        public readonly KafkaGroupMetadata GroupMetadata;

        public KafkaClientGroupInfo([NotNull] string groupName, DateTime timestampUtc, bool isReady, KafkaClientGroupErrorCode? error, [CanBeNull] KafkaGroupMetadata groupMetadata)
        {
            GroupName = groupName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Error = error;
            GroupMetadata = groupMetadata;
        }
    }
}
