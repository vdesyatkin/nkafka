using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionLimitInfo
    {               
        public readonly int? MaxMessageSizeByteCount;

        public readonly int? MaxMessageCount;

        public DateTime TimestampUtc;

        public KafkaProducerTopicPartitionLimitInfo(int? maxMessageSizeByteCount, int? maxMessageCount, DateTime timestampUtc)
        {                        
            MaxMessageSizeByteCount = maxMessageSizeByteCount;
            MaxMessageCount = maxMessageCount;
            TimestampUtc = timestampUtc;
        }
    }
}
