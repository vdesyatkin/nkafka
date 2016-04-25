using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;        
        private int? _batchSizeByteCount;
        private int? _batchMaxMessageCount;
        private int? _maxMessageSizeByteCount;
        private TimeSpan? _batchServerTimeout;
        private TimeSpan? _errorRetryPeriod;

        [NotNull] public static KafkaProducerSettings Default => new KafkaProducerSettingsBuilder().Build();

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetConsistencyLevel(KafkaConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetCodecType(KafkaCodecType codecType)
        {
            _codecType = codecType;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetBatchSizeByteCount(int batchSizeByteCount)
        {
            _batchSizeByteCount = batchSizeByteCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetBatchMaxMessageCount(int batchMaxMessageCount)
        {
            _batchMaxMessageCount = batchMaxMessageCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetMessageSizeByteCount(int maxMessageSizeByteCount)
        {
            _maxMessageSizeByteCount = maxMessageSizeByteCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetBatchServerTimeout(TimeSpan timeout)
        {
            _batchServerTimeout = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettings Build()
        {
            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;            
            var batchSizeByteCount = _batchSizeByteCount ?? 200 * 200;
            var batchMaxMessageCount = _batchMaxMessageCount;
            var maxMessageSizeByteCount = _maxMessageSizeByteCount;
            var batchServerTimeout = _batchServerTimeout ?? TimeSpan.FromSeconds(1);
            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,
                batchSizeByteCount,
                batchMaxMessageCount,
                maxMessageSizeByteCount,
                batchServerTimeout,
                errorRetryPeriod);
        }
    }
}
