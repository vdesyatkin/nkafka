using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;        
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
        public KafkaProducerSettingsBuilder SetBatchByteMaxSizeBytes(int batchMaxSizeBytes)
        {
            _produceBatchMaxSizeBytes = batchMaxSizeBytes;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProduceServerTimeout(TimeSpan timeout)
        {
            _produceTimeout = timeout;
            return this;
        }

        public KafkaProducerSettings Build()
        {
            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;            
            var batchMaxSizeBytes = _produceBatchMaxSizeBytes ?? 200 * 200;
            var produceTimeout = _produceTimeout ?? TimeSpan.FromSeconds(1);

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,                
                batchMaxSizeBytes,
                produceTimeout);
        }
    }
}
