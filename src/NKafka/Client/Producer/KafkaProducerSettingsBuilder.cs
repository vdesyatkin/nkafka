using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;        
        private int? _produceBatchMaxByteCount;
        private int? _produceBatchMaxMessageCount;
        private TimeSpan? _produceTimeout;

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
        public KafkaProducerSettingsBuilder SetBatchMaxSizeBytes(int batchMaxSizeBytes)
        {
            _produceBatchMaxByteCount = batchMaxSizeBytes;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetBatchMaxMessageCount(int batchMaxMessageCount)
        {
            _produceBatchMaxMessageCount = batchMaxMessageCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetProduceServerTimeout(TimeSpan timeout)
        {
            _produceTimeout = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettings Build()
        {
            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;            
            var batchMaxByteCount = _produceBatchMaxByteCount ?? 200 * 200;
            var batchMaxMessageCount = _produceBatchMaxMessageCount;
            var produceTimeout = _produceTimeout ?? TimeSpan.FromSeconds(1);

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,
                batchMaxByteCount,
                batchMaxMessageCount,
                produceTimeout);
        }
    }
}
