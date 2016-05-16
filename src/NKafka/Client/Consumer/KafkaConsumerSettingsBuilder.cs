using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettingsBuilder
    {        
        private int? _consumeBatchMinSizeBytes;
        private int? _consumeBatchMaxSizeBytes;
        private TimeSpan? _consumeServerWaitTime;
        private int? _bufferMaxMessageCount;
        private int? _bufferMaxSizeBytes;
        private TimeSpan? _errorRetryPeriod;

        [NotNull] public static readonly KafkaConsumerSettings Default = new KafkaConsumerSettingsBuilder().Build();

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBatchMinSizeBytes(int sizeBytes)
        {
            _consumeBatchMinSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBatchMaxSizeBytes(int sizeBytes)
        {
            _consumeBatchMaxSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetConsumeServerWaitTime(TimeSpan waitTime)
        {
            _consumeServerWaitTime = waitTime;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxMessageCount(int messageCount)
        {
            _bufferMaxMessageCount = messageCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsBuilder SetBufferMaxSizeBytes(int sizeBytes)
        {
            _bufferMaxSizeBytes = sizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettings Build()
        {
            // https://kafka.apache.org/documentation.html#brokerconfigs;
            var batchMinSizeBytes = _consumeBatchMinSizeBytes ?? 0;
            var batchMaxSizeBytes = _consumeBatchMaxSizeBytes ?? 1048576;
            var consumerServerWaitTime = _consumeServerWaitTime ?? TimeSpan.Zero;
            var bufferMaxMessageCount = _bufferMaxMessageCount;
            var bufferMaxSizeBytes = _bufferMaxSizeBytes ?? 100 * 1024 * 1024;
            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            return new KafkaConsumerSettings(                
                batchMinSizeBytes,
                batchMaxSizeBytes,
                consumerServerWaitTime,
                bufferMaxMessageCount,
                bufferMaxSizeBytes,
                errorRetryPeriod);
        }        
    }
}
