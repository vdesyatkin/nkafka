using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionLimitInfo
    {       
        public DateTime TimestampUtc;

        public readonly int? MaxMessageSizeByteCount;

        public readonly int? MaxMessageCount;        

        public KafkaProducerTopicPartitionLimitInfo(DateTime timestampUtc, int? maxMessageSizeByteCount, int? maxMessageCount)
        {            
            TimestampUtc = timestampUtc;
            MaxMessageSizeByteCount = maxMessageSizeByteCount;
            MaxMessageCount = maxMessageCount;            
        }
    }
}
