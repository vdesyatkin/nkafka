using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicMessagesInfo
    {        
        public readonly long TotalEnqueuedCount;
        public readonly DateTime? EnqueueTimestampUtc;        

        public readonly long SendPendingCount;
        public readonly long TotalSentCount;
        public readonly DateTime? SendTimestampUtc;

        public readonly long TotalFallbackCount;
        public readonly DateTime? FallbackTimestampUtc;

        public KafkaProducerTopicMessagesInfo(
            long totalEnqueuedCount, DateTime? enqueueTimestampUtc,
            long sendPendingCount, long totalSentCount, DateTime? sendTimestampUtc,
            long totalFallbackCount, DateTime? fallbackTimestampUtc)
        {            
            TotalEnqueuedCount = totalEnqueuedCount;
            EnqueueTimestampUtc = enqueueTimestampUtc;
            
            SendPendingCount = sendPendingCount;
            TotalSentCount = totalSentCount;
            SendTimestampUtc = sendTimestampUtc;

            TotalFallbackCount = totalFallbackCount;
            FallbackTimestampUtc = fallbackTimestampUtc;
        }
    }
}
