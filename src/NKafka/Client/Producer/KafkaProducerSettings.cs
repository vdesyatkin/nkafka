using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettings
    {        
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;        
        public readonly int BatchMaxSizeByteCount;
        public readonly int? BatchMaxMessageCount;        
        public readonly int PartitionBatchSizeByteCount;        
        public readonly int MessageMaxSizeByteCount;
        public readonly TimeSpan BatchServerTimeout;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaProducerSettings(         
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,          
          int batchMaxSizeByteCount,
          int? batchMaxMessageCount,
          int partitionBatchSizeByteCount,
          int messageMaxSizeByteCount,
          TimeSpan batchServerTimeout,
          TimeSpan errorRetryPeriod
          )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;                   
            BatchMaxSizeByteCount = batchMaxSizeByteCount;
            BatchMaxMessageCount = batchMaxMessageCount;
            PartitionBatchSizeByteCount = partitionBatchSizeByteCount;
            MessageMaxSizeByteCount = messageMaxSizeByteCount;
            BatchServerTimeout = batchServerTimeout;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}
