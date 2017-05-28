using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettingsBuilder
    {
        // https://kafka.apache.org/documentation.html#brokerconfigs;        

        public static readonly KafkaConsumerBeginBehavior DefaultBeginBehavior = KafkaConsumerBeginBehavior.BeginFromMinAvailableOffset;
        public static readonly int DefaultTopicBatchMinSizeBytes = 1;
        public static readonly int DefaultPartitionBatchMaxSizeBytes = 1048576;
        public static readonly TimeSpan DefaultFetchServerWaitTime = TimeSpan.FromMilliseconds(500);
        public static readonly TimeSpan DefaultFetchClientTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultOffsetRequestTimeout = TimeSpan.FromSeconds(30);
        public static readonly long DefaultBufferMaxSizeBytes = 100 * DefaultPartitionBatchMaxSizeBytes;
        public static readonly TimeSpan DefaultErrorRetryPeriod = TimeSpan.FromSeconds(10);

        [NotNull] public static readonly KafkaConsumerSettings Default = new KafkaConsumerSettingsBuilder().Build();

        private KafkaConsumerBeginBehavior? _beginBehavior;
        private int? _topicBatchMinSizeBytes;
        private int? _topicBatchMaxSizeBytes;
        private int? _partitionBatchMaxSizeBytes;
        private TimeSpan? _fetchServerWaitTime;
        private TimeSpan? _fetchClientTimeout;
        private TimeSpan? _offsetRequestTimesout;
        private int? _bufferMaxSizeBytes;
        private int? _bufferMaxMessageCount;
        private TimeSpan? _errorRetryPeriod;

        [NotNull]
        public KafkaConsumerSettingsBuilder SetBeginBehaviour(KafkaConsumerBeginBehavior beginBehavior)
        {
            _beginBehavior = beginBehavior;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetTopicBatchMinSizeBytes(int sizeBytes)
        {
            _topicBatchMinSizeBytes = sizeBytes;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetTopicBatchMaxSizeBytes(int sizeBytes)
        {
            _topicBatchMaxSizeBytes = sizeBytes;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetPartitionBatchMaxSizeBytes(int sizeBytes)
        {
            _partitionBatchMaxSizeBytes = sizeBytes;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetFetchServerWaitTime(TimeSpan waitTime)
        {
            _fetchServerWaitTime = waitTime;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetFetchClientTimeout(TimeSpan timeout)
        {
            _fetchClientTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetOffsetRequestTimeout(TimeSpan timeout)
        {
            _offsetRequestTimesout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxSizeBytes(int sizeBytes)
        {
            _bufferMaxSizeBytes = sizeBytes;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxMessageCount(int messageCount)
        {
            _bufferMaxMessageCount = messageCount;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettings Build()
        {
            var beginBehavior = _beginBehavior ?? DefaultBeginBehavior;
            var topicBatchMinSizeBytes = _topicBatchMinSizeBytes ?? DefaultTopicBatchMinSizeBytes;
            var topicBatchMaxSizeBytes = _topicBatchMaxSizeBytes;
            var partitionBatchMaxSizeBytes = _partitionBatchMaxSizeBytes ?? DefaultPartitionBatchMaxSizeBytes;
            var fetchServerWaitTime = _fetchServerWaitTime ?? DefaultFetchServerWaitTime;
            var fetchClientTimeout = _fetchClientTimeout ?? DefaultFetchClientTimeout;
            var offsetRequestTimeout = _offsetRequestTimesout ?? DefaultOffsetRequestTimeout;
            var bufferMaxSizeBytes = _bufferMaxSizeBytes ?? DefaultBufferMaxSizeBytes;
            var bufferMaxMessageCount = _bufferMaxMessageCount;
            var errorRetryPeriod = _errorRetryPeriod ?? DefaultErrorRetryPeriod;

            if (topicBatchMinSizeBytes <= 0)
            {
                topicBatchMinSizeBytes = 0;
                fetchServerWaitTime = TimeSpan.Zero;
            }

            return new KafkaConsumerSettings(
                beginBehavior,
                topicBatchMinSizeBytes,
                topicBatchMaxSizeBytes,
                partitionBatchMaxSizeBytes,
                fetchServerWaitTime,
                fetchClientTimeout,
                offsetRequestTimeout,
                bufferMaxSizeBytes,
                bufferMaxMessageCount,
                errorRetryPeriod);
        }
    }
}