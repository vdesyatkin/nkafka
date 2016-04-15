using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicLimitInfo
    {
        [NotNull]
        public readonly string TopicName;

        public DateTime TimestampUtc;

        public readonly int BatchMaxMessageCount;

        public readonly int BatchMaxSizeBytes;

        public KafkaProducerTopicLimitInfo([NotNull] string topicName, DateTime timestampUtc, int batchMaxMessageCount, int batchMaxSizeBytes)
        {
            TopicName = topicName;
            TimestampUtc = timestampUtc;
            BatchMaxMessageCount = batchMaxMessageCount;
            BatchMaxSizeBytes = batchMaxSizeBytes;
        }
    }
}
