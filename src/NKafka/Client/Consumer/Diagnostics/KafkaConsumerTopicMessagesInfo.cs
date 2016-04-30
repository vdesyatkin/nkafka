using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicMessagesInfo
    {
        public readonly int EnqueuedCount;
        public readonly long TotalReceivedCount;
        public readonly DateTime? ReceiveTimestampUtc;

        public readonly long TotalConsumedCount;
        public readonly DateTime? ConsumeTimestampUtc;

        public readonly long TotalCommitedCount;
        public readonly DateTime? CommitTimestampUtc;

        public KafkaConsumerTopicMessagesInfo(int enqueuedCount, long totalReceivedCount, DateTime? receiveTimestampUtc, 
            long totalConsumedCount, DateTime? consumeTimestampUtc, 
            long totalCommitedCount, DateTime? commitTimestampUtc)
        {
            EnqueuedCount = enqueuedCount;
            TotalReceivedCount = totalReceivedCount;
            ReceiveTimestampUtc = receiveTimestampUtc;
            TotalConsumedCount = totalConsumedCount;
            ConsumeTimestampUtc = consumeTimestampUtc;
            TotalCommitedCount = totalCommitedCount;
            CommitTimestampUtc = commitTimestampUtc;
        }
    }
}
