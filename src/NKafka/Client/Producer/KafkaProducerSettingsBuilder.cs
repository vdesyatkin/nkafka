using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;        
        private int? _produceBatchMaxSizeBytes;
        private TimeSpan? _produceTimeout;

        public static KafkaProducerSettings Default => new KafkaProducerSettingsBuilder().Build();

        public KafkaProducerSettingsBuilder SetConsistencyLevel(KafkaConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        public KafkaProducerSettingsBuilder SetCodecType(KafkaCodecType codecType)
        {
            _codecType = codecType;
            return this;
        }        

        public KafkaProducerSettingsBuilder SetBatchMaxSizeBytes(int batchMaxSizeBytes)
        {
            _produceBatchMaxSizeBytes = batchMaxSizeBytes;
            return this;
        }

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
