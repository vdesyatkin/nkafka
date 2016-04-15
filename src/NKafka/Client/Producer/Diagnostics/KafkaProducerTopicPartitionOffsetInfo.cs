using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionOffsetInfo
    {        
        public DateTime TimestampUtc;

        public long MinOffset;

        public long MaxOffset;

        public KafkaProducerTopicPartitionOffsetInfo(DateTime timestampUtc, long minOffset, long maxOffset)
        {            
            TimestampUtc = timestampUtc;
            MinOffset = minOffset;
            MaxOffset = maxOffset;
        }
    }
}
