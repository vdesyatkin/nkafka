using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettings
    {        
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;             
        public readonly int ProduceBatchMaxByteCount;
        public readonly int? ProduceBatchMaxMessageCount;
        public readonly TimeSpan ProduceServerTimeout;

        public KafkaProducerSettings(         
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,          
          int produceBatchMaxByteCount,
          int? produceBatchMaxMessageCount,
          TimeSpan produceServerTimeout
          )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;            
            ProduceBatchMaxByteCount = produceBatchMaxByteCount;
            ProduceBatchMaxMessageCount = produceBatchMaxMessageCount;
            ProduceServerTimeout = produceServerTimeout;
        }
    }
}
