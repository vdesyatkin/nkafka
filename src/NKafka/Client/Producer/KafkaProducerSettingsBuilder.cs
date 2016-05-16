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
        private int? _partitionBatchSizeByteCount;
        private int? _messageMaxSizeByteCount;
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
        public KafkaProducerSettingsBuilder SetBatchSizeByteCount(int byteCount)
        {
            _batchSizeByteCount = byteCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetBatchMaxMessageCount(int messageCount)
        {
            _batchMaxMessageCount = messageCount;
            return this;
        }        

        [PublicAPI, NotNull]
        public KafkaProducerSettingsBuilder SetMessageMaxSizeByteCount(int maxMessageSizeByteCount)
        {
            _messageMaxSizeByteCount = maxMessageSizeByteCount;
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
            // https://kafka.apache.org/documentation.html#brokerconfigs
            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;            
            var batchSizeByteCount = _batchSizeByteCount ?? 16384;
            var batchMaxMessageCount = _batchMaxMessageCount ?? 1048576;            
            var messageMaxSizeByteCount = _messageMaxSizeByteCount ?? 1000012; //todo (E006) Use protocol header size for it
            var batchServerTimeout = _batchServerTimeout ?? TimeSpan.FromSeconds(1); //todo (E006) 30 seconds by default, but it's too long.
            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            return new KafkaProducerSettings(
                consistencyLevel,
                codecType,
                batchSizeByteCount,
                batchMaxMessageCount,                
                messageMaxSizeByteCount,
                batchServerTimeout,
                errorRetryPeriod);
        }
    }
}
