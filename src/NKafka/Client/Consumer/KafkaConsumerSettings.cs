using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettings
    {
        public KafkaConsumerBeginBehavior BeginBehavior;
        public readonly int TopicBatchMinSizeBytes;
        public readonly int? TopicBatchMaxSizeBytes;
        public readonly int PartitionBatchMaxSizeBytes;
        public readonly TimeSpan FetchServerWaitTime;
        public readonly TimeSpan FetchClientTimeout;
        public readonly TimeSpan OffestRequestTimeout;
        public readonly long BufferMaxSizeBytes;
        public readonly int? BufferMaxMessageCount;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaConsumerSettings(
          KafkaConsumerBeginBehavior beginBehavior,
          int topicBatchMinSizeBytes,
          int? topicBatchMaxSizeBytes,
          int partitionBatchMaxSizeBytes,
          TimeSpan fetchServerWaitTime,
          TimeSpan fetchClientTimeout,
          TimeSpan offestRequestTimeout,
          long bufferMaxSizeBytes,
          int? bufferMaxMesageCount,
          TimeSpan errorRetryPeriod
        )
        {
            BeginBehavior = beginBehavior;
            TopicBatchMinSizeBytes = topicBatchMinSizeBytes;
            TopicBatchMaxSizeBytes = topicBatchMaxSizeBytes;
            PartitionBatchMaxSizeBytes = partitionBatchMaxSizeBytes;
            FetchServerWaitTime = fetchServerWaitTime;
            FetchClientTimeout = fetchClientTimeout;
            OffestRequestTimeout = offestRequestTimeout;
            BufferMaxSizeBytes = bufferMaxSizeBytes;
            BufferMaxMessageCount = bufferMaxMesageCount;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}