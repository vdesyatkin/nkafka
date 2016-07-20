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
        public readonly TimeSpan? FetchTimeout;
        public readonly long BufferMaxSizeBytes;
        public readonly int? BufferMaxMessageCount;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaConsumerSettings(
          KafkaConsumerBeginBehavior beginBehavior,
          int topicBatchMinSizeBytes,
          int partitionBatchMaxSizeBytes,
          TimeSpan fetchServerWaitTime,
          TimeSpan? fetchTimeout,
          long bufferMaxSizeBytes,
          int? bufferMaxMesageCount,
          TimeSpan errorRetryPeriod
          )
        {
            BeginBehavior = beginBehavior;
            TopicBatchMinSizeBytes = topicBatchMinSizeBytes;
            PartitionBatchMaxSizeBytes = partitionBatchMaxSizeBytes;
            FetchServerWaitTime = fetchServerWaitTime;
            FetchTimeout = fetchTimeout;
            BufferMaxSizeBytes = bufferMaxSizeBytes;
            BufferMaxMessageCount = bufferMaxMesageCount;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}