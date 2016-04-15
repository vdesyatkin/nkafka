using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicMessageCountInfo
    {
        public readonly long EnqueuedMessageCount;

        public readonly DateTime? EnqueueTimestampUtc;

        public readonly long SentMessageCount;

        public readonly DateTime? SendTimestampUtc;

        public KafkaProducerTopicMessageCountInfo(long enqueuedMessageCount, DateTime? enqueueTimestampUtc, long sentMessageCount, DateTime? sendTimestampUtc)
        {
            EnqueuedMessageCount = enqueuedMessageCount;
            EnqueueTimestampUtc = enqueueTimestampUtc;
            SentMessageCount = sentMessageCount;
            SendTimestampUtc = sendTimestampUtc;
        }
    }
}
