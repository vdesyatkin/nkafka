using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettingsBuilder
    {
        // https://kafka.apache.org/documentation.html#brokerconfigs        

        public static readonly KafkaConsistencyLevel DefaultConsistencyLevel = KafkaConsistencyLevel.OneReplica;
        public static readonly KafkaCodecType DefaultCodecType = KafkaCodecType.CodecNone;
        public static readonly int DefaultMessageMaxSizeByteCount = 999990; //1000012 in original - 22 bytes reserved for headers
        public static readonly int DefaultPartitionBatchPreferredSizeByteCount = 16384;
        public static readonly int DefaultPartitionBatchMaxSizeByteCount = 32 * DefaultPartitionBatchPreferredSizeByteCount; // multiplier = 64 in original - total cap for all request bytes
        public static readonly int DefaultProduceRequestMaxSizeByteCount = 32 * DefaultPartitionBatchPreferredSizeByteCount;
        public static readonly TimeSpan DefaultProduceRequestServerTimeout = TimeSpan.FromSeconds(3); // 30 seconds in original - too long.
        public static readonly TimeSpan DefaultProduceRequestClientTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultErrorRetryPeriod = TimeSpan.FromSeconds(10);

        [NotNull]
        public static readonly KafkaProducerSettings Default = new KafkaProducerSettingsBuilder().Build();

        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;
        private int? _messageMaxSizeByteCount;
        private int? _partitionBatchPreferredSizeByteCount;
        private int? _partitionBatchMaxSizeByteCount;
        private int? _produceRequestMaxSizeByteCount;
        private TimeSpan? _produceServerTimeout;
        private TimeSpan? _produceClientTimeout;
        private TimeSpan? _errorRetryPeriod;

        [NotNull]
        public KafkaProducerSettingsBuilder SetConsistencyLevel(KafkaConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetCodecType(KafkaCodecType codecType)
        {
            _codecType = codecType;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetMessageMaxSizeByteCount(int byteCount)
        {
            _messageMaxSizeByteCount = byteCount;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetPartitionBatchPreferredSizeByteCount(int byteCount)
        {
            _partitionBatchPreferredSizeByteCount = byteCount;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetPartitionBatchMaxSizeByteCount(int byteCount)
        {
            _partitionBatchMaxSizeByteCount = byteCount;
            return this;
        }


        [NotNull]
        public KafkaProducerSettingsBuilder SetProduceRequestMaxSizeByteCount(int byteCount)
        {
            _produceRequestMaxSizeByteCount = byteCount;
            return this;
        }


        [NotNull]
        public KafkaProducerSettingsBuilder SetProduceRequestServerTimeout(TimeSpan timeout)
        {
            _produceServerTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetProduceRequestClientTimeout(TimeSpan timeout)
        {
            _produceClientTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaProducerSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaProducerSettings Build()
        {
            var consistencyLevel = _consistencyLevel ?? DefaultConsistencyLevel;
            var codecType = _codecType ?? DefaultCodecType;
            var messageMaxSizeByteCount = _messageMaxSizeByteCount ?? DefaultMessageMaxSizeByteCount;
            var partitionBatchPreferredSizeByteCount = _partitionBatchPreferredSizeByteCount ?? DefaultPartitionBatchPreferredSizeByteCount;
            var partitionBatchMaxSizeByteCount = _partitionBatchMaxSizeByteCount ?? DefaultPartitionBatchMaxSizeByteCount;
            var produceRequestMaxSizeByteCount = _produceRequestMaxSizeByteCount ?? DefaultProduceRequestMaxSizeByteCount;
            var produceRequestServerTimeout = _produceServerTimeout ?? DefaultProduceRequestServerTimeout;
            var produceRequestClientTimeout = _produceServerTimeout ?? DefaultProduceRequestClientTimeout;
            var errorRetryPeriod = _errorRetryPeriod ?? DefaultErrorRetryPeriod;

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,
                messageMaxSizeByteCount,
                partitionBatchPreferredSizeByteCount,
                partitionBatchMaxSizeByteCount,
                produceRequestMaxSizeByteCount,
                produceRequestServerTimeout,
                produceRequestClientTimeout,
                errorRetryPeriod);
        }
    }
}