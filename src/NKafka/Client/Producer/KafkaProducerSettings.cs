using System;

namespace NKafka.Client.Producer
{
    public sealed class KafkaProducerSettings
    {        
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;     
        public readonly TimeSpan ProducePeriod;
        public readonly int ProduceBatchMaxSizeBytes;
        public readonly TimeSpan ProduceTimeout;

        public KafkaProducerSettings(         
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,
          TimeSpan producePeriod,
          int produceBatchMaxSizeLimit,         
          TimeSpan produceTimeout
          )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;
            ProducePeriod = producePeriod;
            ProduceBatchMaxSizeBytes = produceBatchMaxSizeLimit;
            ProduceTimeout = produceTimeout;
        }
    }
}
