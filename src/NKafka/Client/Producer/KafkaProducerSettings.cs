using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettings
    {        
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;        
        public readonly int BatchSizeByteCount;        
        public readonly int? BatchMaxMessageCount;
        public readonly int? MaxMessageSizeByteCount;
        public readonly TimeSpan BatchServerTimeout;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaProducerSettings(         
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,          
          int batchSizeByteCount,
          int? batchMaxMessageCount,
          int? maxMessageSizeByteCount,
          TimeSpan batchServerTimeout,
          TimeSpan errorRetryPeriod
          )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;                   
            BatchSizeByteCount = batchSizeByteCount;
            BatchMaxMessageCount = batchMaxMessageCount;
            MaxMessageSizeByteCount = maxMessageSizeByteCount;
            BatchServerTimeout = batchServerTimeout;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}
