using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionLimitInfo
    {       
        public DateTime TimestampUtc;

        public readonly int BatchMaxByteCount;

        public readonly int? BatchMaxMessageCount;        

        public KafkaProducerTopicPartitionLimitInfo(DateTime timestampUtc, int batchMaxByteCount, int? batchMaxMessageCount)
        {            
            TimestampUtc = timestampUtc;
            BatchMaxByteCount = batchMaxByteCount;
            BatchMaxMessageCount = batchMaxMessageCount;            
        }
    }
}
