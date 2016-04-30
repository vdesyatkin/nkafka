using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicMessagesInfo
    {
        public readonly long EnqueuedMessageCount;

        public readonly long TotalEnqueuedMessageCount;

        public readonly DateTime? EnqueueTimestampUtc;                

        public readonly long TotalFallbackMessageCount;

        public readonly DateTime? FallbackTimestampUtc;

        public readonly long TotalSentMessageCount;

        public readonly DateTime? SendTimestampUtc;

        public KafkaProducerTopicMessagesInfo(
            long enqueuedMessageCount, long totalEnqueuedMessageCount, DateTime? enqueueTimestampUtc,
            long totalFallbackMessageCount, DateTime? fallbackTimestampUtc,
            long totalSentMessageCount, DateTime? sendTimestampUtc)
        {
            EnqueuedMessageCount = enqueuedMessageCount;
            TotalEnqueuedMessageCount = totalEnqueuedMessageCount;
            EnqueueTimestampUtc = enqueueTimestampUtc;
            TotalFallbackMessageCount = totalFallbackMessageCount;
            FallbackTimestampUtc = fallbackTimestampUtc;
            TotalSentMessageCount = totalSentMessageCount;
            SendTimestampUtc = sendTimestampUtc;
        }
    }
}
