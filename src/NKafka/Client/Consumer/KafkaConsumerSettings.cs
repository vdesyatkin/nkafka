using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettings
    {
        public KafkaConsumerBeginBehavior BeginBehavior;
        public readonly int TopicBatchMinSizeBytes;
        public readonly int PartitionBatchMaxSizeBytes;
        public readonly TimeSpan FetchServerWaitTime;
        public readonly long BufferMaxSizeBytes;
        public readonly int? BufferMaxMessageCount;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaConsumerSettings(
          KafkaConsumerBeginBehavior beginBehavior,
          int topicBatchMinSizeBytes,
          int partitionBatchMaxSizeBytes,
          TimeSpan fetchServerWaitTime,
          long bufferMaxSizeBytes,
          int? bufferMaxMesageCount,          
          TimeSpan errorRetryPeriod
          )
        {
            BeginBehavior = beginBehavior;
            TopicBatchMinSizeBytes = topicBatchMinSizeBytes;
            PartitionBatchMaxSizeBytes = partitionBatchMaxSizeBytes;
            FetchServerWaitTime = fetchServerWaitTime;
            BufferMaxSizeBytes = bufferMaxSizeBytes;
            BufferMaxMessageCount = bufferMaxMesageCount;            
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}
