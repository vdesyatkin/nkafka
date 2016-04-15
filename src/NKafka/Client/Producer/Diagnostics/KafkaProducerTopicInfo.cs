using System;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        public readonly DateTime TimestampUtc;

        public readonly bool IsProducing;

        public readonly KafkaProducerTopicErrorCode? Error;

        public readonly KafkaProducerTopicMessageCountInfo EnqueuedMessageCount;                

        public readonly KafkaProducerTopicMessageCountInfo TotalSentMessageCount;        

        [NotNull]
        public readonly KafkaClientTopicInfo TopicInfo;        

        [NotNull]
        public readonly KafkaProducerTopicLimitInfo TopicBatchLimitInfo;

        [CanBeNull]
        public readonly KafkaProdcuerTopicOffsetInfo TopicOffsetInfo;

        public KafkaProducerTopicInfo([NotNull] string topicName, DateTime timestampUtc, 
            bool isProducing, KafkaProducerTopicErrorCode? error, 
            KafkaProducerTopicMessageCountInfo enqueuedMessageCount, 
            KafkaProducerTopicMessageCountInfo totalSentMessageCount, 
            [NotNull] KafkaClientTopicInfo topicInfo,
            [NotNull] KafkaProducerTopicLimitInfo topicBatchLimitInfo,
            [CanBeNull] KafkaProdcuerTopicOffsetInfo topicOffsetInfo)
        {
            TopicName = topicName;
            TimestampUtc = timestampUtc;
            IsProducing = isProducing;
            Error = error;
            EnqueuedMessageCount = enqueuedMessageCount;
            TotalSentMessageCount = totalSentMessageCount;
            TopicInfo = topicInfo;
            TopicOffsetInfo = topicOffsetInfo;
            TopicBatchLimitInfo = topicBatchLimitInfo;
        }
    }
}
