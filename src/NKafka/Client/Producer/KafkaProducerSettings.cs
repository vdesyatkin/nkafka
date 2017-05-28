using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettings
    {
        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;
        public readonly int MessageMaxSizeByteCount;
        public readonly int PartitionBatchPreferredSizeByteCount;
        public readonly int PartitionBatchMaxSizeByteCount;
        public readonly int ProduceRequestMaxSizeByteCount;
        public readonly TimeSpan ProduceRequestServerTimeout;
        public readonly TimeSpan ProduceRequestClientTimeout;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaProducerSettings(
          KafkaConsistencyLevel consistencyLevel,
          KafkaCodecType codecType,
          int messageMaxSizeByteCount,
          int partitionBatchPreferredSizeByteCount,
          int partitionBatchMaxSizeByteCount,
          int produceRequestMaxSizeByteCount,
          TimeSpan produceRequestServerTimeout,
          TimeSpan produceRequestClientTimeout,
          TimeSpan errorRetryPeriod
        )
        {
            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;
            MessageMaxSizeByteCount = messageMaxSizeByteCount;
            PartitionBatchPreferredSizeByteCount = partitionBatchPreferredSizeByteCount;
            PartitionBatchMaxSizeByteCount = partitionBatchMaxSizeByteCount;
            ProduceRequestServerTimeout = produceRequestServerTimeout;
            ProduceRequestClientTimeout = produceRequestClientTimeout;
            ProduceRequestMaxSizeByteCount = produceRequestMaxSizeByteCount;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}