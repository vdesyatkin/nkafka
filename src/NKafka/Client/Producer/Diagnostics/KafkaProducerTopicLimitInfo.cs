using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicLimitInfo
    {       
        public DateTime TimestampUtc;

        public readonly int BatchMaxMessageCount;

        public readonly int BatchMaxSizeBytes;

        public KafkaProducerTopicLimitInfo(DateTime timestampUtc, int batchMaxMessageCount, int batchMaxSizeBytes)
        {            
            TimestampUtc = timestampUtc;
            BatchMaxMessageCount = batchMaxMessageCount;
            BatchMaxSizeBytes = batchMaxSizeBytes;
        }
    }
}
