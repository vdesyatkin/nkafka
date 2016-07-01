using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionLimitInfo
    {               
        public readonly int? MaxMessageSizeByteCount;

        public readonly int? MaxBatchSizeByteCount;

        public DateTime TimestampUtc;

        public KafkaProducerTopicPartitionLimitInfo(int? maxMessageSizeByteCount, int? maxBatchSizeByteCount, DateTime timestampUtc)
        {                        
            MaxMessageSizeByteCount = maxMessageSizeByteCount;
            MaxBatchSizeByteCount = maxBatchSizeByteCount;
            TimestampUtc = timestampUtc;
        }
    }
}
