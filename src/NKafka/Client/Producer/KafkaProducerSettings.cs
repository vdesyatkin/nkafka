using System;

namespace NKafka.Client.Producer
{
    public sealed class KafkaProducerSettings
    {        
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;             
        public readonly int ProduceBatchMaxSizeBytes;
        public readonly TimeSpan ProduceServerTimeout;

        public KafkaProducerSettings(         
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,          
          int produceBatchMaxSizeLimit,         
          TimeSpan produceServerTimeout
          )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;            
            ProduceBatchMaxSizeBytes = produceBatchMaxSizeLimit;
            ProduceServerTimeout = produceServerTimeout;
        }
    }
}
