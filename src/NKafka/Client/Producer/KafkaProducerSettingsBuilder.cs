using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;
        private TimeSpan? _producePeriod;
        private int? _produceBatchMaxSizeBytes;
        private TimeSpan? _produceTimeout;

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetConsistencyLevel(KafkaConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetCodecType(KafkaCodecType codecType)
        {
            _codecType = codecType;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProducePeriod(TimeSpan producePeriod)
        {
            _producePeriod = producePeriod;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetBatchByteMaxSizeBytes(int batchMaxSizeBytes)
        {
            _produceBatchMaxSizeBytes = batchMaxSizeBytes;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProduceTimeout(TimeSpan produceTimeout)
        {
            _produceTimeout = produceTimeout;
            return this;
        }

        public KafkaProducerSettings Build()
        {
            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;
            var producePeriod = _producePeriod ?? TimeSpan.FromSeconds(1);
            var batchMaxSizeBytes = _produceBatchMaxSizeBytes ?? 200 * 200;
            var produceTimeout = _produceTimeout ?? TimeSpan.FromSeconds(1);

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,
                producePeriod,
                batchMaxSizeBytes,
                produceTimeout);
        }
    }
}
