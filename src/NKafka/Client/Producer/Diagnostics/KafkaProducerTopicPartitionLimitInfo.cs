using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionLimitInfo
    {       
        public DateTime TimestampUtc;

        public readonly int? MaxMessageSize;

        public readonly int? MaxMessageCount;        

        public KafkaProducerTopicPartitionLimitInfo(DateTime timestampUtc, int? maxMessageSize, int? maxMessageCount)
        {            
            TimestampUtc = timestampUtc;
            MaxMessageSize = maxMessageSize;
            MaxMessageCount = maxMessageCount;            
        }
    }
}
