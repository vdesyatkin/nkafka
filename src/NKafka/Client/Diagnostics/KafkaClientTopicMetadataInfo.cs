using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientTopicMetadataInfo
    {
        [NotNull] public readonly string TopicName;

        [CanBeNull] public readonly string ClusterId;

        public readonly bool IsReady;

        public readonly KafkaClientTopicMetadataErrorCode? Error;

        [CanBeNull] public readonly KafkaTopicMetadata TopicMetadata;

        public readonly DateTime TimestampUtc;

        public KafkaClientTopicMetadataInfo([NotNull] string topicName,
            [CanBeNull] string clusterId,
            bool isReady,
            KafkaClientTopicMetadataErrorCode? error,
            [CanBeNull] KafkaTopicMetadata topicMetadata,
            DateTime timestampUtc)
        {
            TopicName = topicName;
            ClusterId = clusterId;
            IsReady = isReady;
            Error = error;
            TopicMetadata = topicMetadata;
            TimestampUtc = timestampUtc;
        }
    }
}
