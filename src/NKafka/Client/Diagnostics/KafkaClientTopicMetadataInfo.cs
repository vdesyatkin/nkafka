using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientTopicMetadataInfo
    {
        [NotNull]
        public readonly string TopicName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaClientTopicMetadataErrorCode? Error;

        [CanBeNull]
        public readonly KafkaTopicMetadata TopicMetadata;

        public KafkaClientTopicMetadataInfo([NotNull] string topicName, DateTime timestampUtc, bool isReady, KafkaClientTopicMetadataErrorCode? error, [CanBeNull] KafkaTopicMetadata topicMetadata)
        {
            TopicName = topicName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Error = error;
            TopicMetadata = topicMetadata;
        }
    }
}
