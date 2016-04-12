using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;

        public readonly KafkaClientTopicErrorCode? Error;

        [CanBeNull]
        public readonly KafkaTopicMetadata TopicMetadata;

        public KafkaClientTopicInfo([NotNull] string topicName, DateTime timestampUtc, bool isReady, KafkaClientTopicErrorCode? error, [CanBeNull] KafkaTopicMetadata topicMetadata)
        {
            TopicName = topicName;
            TimestampUtc = timestampUtc;
            IsReady = isReady;
            Error = error;
            TopicMetadata = topicMetadata;
        }
    }
}
