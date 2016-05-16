using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettings
    {        
        public readonly int TopicBatchMinSizeBytes;
        public readonly int PartitionBatchMaxSizeBytes;
        public readonly TimeSpan FetchServerWaitTime;
        public readonly long BufferMaxSizeBytes;
        public readonly int? BufferMaxMessageCount;        
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaConsumerSettings(          
          int topicBatchMinSizeBytes,
          int partitionBatchMaxSizeBytes,
          TimeSpan fetchServerWaitTime,
          long bufferMaxSizeBytes,
          int? bufferMaxMesageCount,          
          TimeSpan errorRetryPeriod
          )
        {            
            TopicBatchMinSizeBytes = topicBatchMinSizeBytes;
            PartitionBatchMaxSizeBytes = partitionBatchMaxSizeBytes;
            FetchServerWaitTime = fetchServerWaitTime;
            BufferMaxSizeBytes = bufferMaxSizeBytes;
            BufferMaxMessageCount = bufferMaxMesageCount;            
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}
