using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettingsBuilder
    {
        // https://kafka.apache.org/documentation.html#brokerconfigs;

        [NotNull]
        public static readonly KafkaConsumerSettings Default = new KafkaConsumerSettingsBuilder().Build();

        public readonly static KafkaConsumerBeginBehavior DefaultBeginBehavior = KafkaConsumerBeginBehavior.BeginFromMinAvailableOffset;
        public readonly static int DefaultTopicBatchMinSizeBytes = 1;
        public readonly static int DefaultPartitionBatchMaxSizeBytes = 1048576;
        public readonly static TimeSpan DefaultFetchServerWaitTime = TimeSpan.FromMilliseconds(100);
        public readonly static long DefaultBufferMaxSizeBytes = 100 * DefaultPartitionBatchMaxSizeBytes;        
        public readonly static TimeSpan DefaultErrorRetryPeriod = TimeSpan.FromSeconds(10);

        private KafkaConsumerBeginBehavior? _beginBehavior;
        private int? _topicBatchMinSizeBytes;
        private int? _partitionBatchMaxSizeBytes;
        private TimeSpan? _fetchServerWaitTime;
        private int? _bufferMaxSizeBytes;
        private int? _bufferMaxMessageCount;
        private TimeSpan? _errorRetryPeriod;

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBeginBehaviour(KafkaConsumerBeginBehavior beginBehavior)
        {
            _beginBehavior = beginBehavior;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetTopicBatchMinSizeBytes(int sizeBytes)
        {
            _topicBatchMinSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetPartitionBatchMaxSizeBytes(int sizeBytes)
        {
            _partitionBatchMaxSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetFetchServerWaitTime(TimeSpan waitTime)
        {
            _fetchServerWaitTime = waitTime;
            return this;
        }        

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxSizeBytes(int sizeBytes)
        {
            _bufferMaxSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxMessageCount(int messageCount)
        {
            _bufferMaxMessageCount = messageCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettings Build()
        {
            var beginBehavior = _beginBehavior ?? DefaultBeginBehavior;
            var topicBatchMinSizeBytes = _topicBatchMinSizeBytes ?? DefaultTopicBatchMinSizeBytes;
            var partitionBatchMaxSizeBytes = _partitionBatchMaxSizeBytes ?? DefaultPartitionBatchMaxSizeBytes;
            var fetchServerWaitTime = _fetchServerWaitTime ?? DefaultFetchServerWaitTime;
            var bufferMaxSizeBytes = _bufferMaxSizeBytes ?? DefaultBufferMaxSizeBytes;
            var bufferMaxMessageCount = _bufferMaxMessageCount;
            var errorRetryPeriod = _errorRetryPeriod ?? DefaultErrorRetryPeriod;

            return new KafkaConsumerSettings(
                beginBehavior,           
                topicBatchMinSizeBytes,
                partitionBatchMaxSizeBytes,
                fetchServerWaitTime,
                bufferMaxSizeBytes,
                bufferMaxMessageCount,                
                errorRetryPeriod);
        }        
    }
}
