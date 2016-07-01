using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicMessageCountInfo
    {        
        public readonly long? ReceivePendingCount;
        public readonly long TotalReceivedCount;
        public readonly DateTime? ReceiveTimestampUtc;

        public readonly long ConsumePendingCount;
        public readonly long TotalConsumedCount;
        public readonly DateTime? ConsumeTimestampUtc;

        public readonly long ClientCommitPendingCount;        
        public readonly long TotalClientCommitedCount;
        public readonly DateTime? ClientCommitTimestampUtc;

        public readonly long ServerCommitPendingCount;
        public readonly long TotalServerCommitedCount;
        public readonly DateTime? ServerCommitTimestampUtc;

        public KafkaConsumerTopicMessageCountInfo(
            long? receivePendingCount, long totalReceivedCount, DateTime? receiveTimestampUtc, 
            long consumePendingCount, long totalConsumedCount, DateTime? consumeTimestampUtc, 
            long clientCommitPendingCount, long totalClientCommitedCount, DateTime? clientCommitTimestampUtc, 
            long serverCommitPendingCount, long totalServerCommitedCount, DateTime? serverCommitTimestampUtc)
        {
            ReceivePendingCount = receivePendingCount;
            TotalReceivedCount = totalReceivedCount;
            ReceiveTimestampUtc = receiveTimestampUtc;
            ConsumePendingCount = consumePendingCount;
            TotalConsumedCount = totalConsumedCount;
            ConsumeTimestampUtc = consumeTimestampUtc;
            ClientCommitPendingCount = clientCommitPendingCount;
            TotalClientCommitedCount = totalClientCommitedCount;
            ClientCommitTimestampUtc = clientCommitTimestampUtc;
            ServerCommitPendingCount = serverCommitPendingCount;
            TotalServerCommitedCount = totalServerCommitedCount;
            ServerCommitTimestampUtc = serverCommitTimestampUtc;
        }
    }
}
