using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaConsumerSettingsBuilder
    {        
        private int? _consumeBatchMinSizeBytes;
        private TimeSpan? _consumeTimeout;        

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetBatchByteMinSizeBytes(int batchMinSizeBytes)
        {
            _consumeBatchMinSizeBytes = batchMinSizeBytes;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumeServerTimeout(TimeSpan timeout)
        {
            _consumeTimeout = timeout;
            return this;
        }

        public KafkaConsumerSettings Build()
        {            
            var batchMinSizeBytes = _consumeBatchMinSizeBytes ?? 200 * 200;
            var consumeTimeout = _consumeTimeout ?? TimeSpan.Zero;

            return new KafkaConsumerSettings(                
                batchMinSizeBytes,
                consumeTimeout);
        }
    }
}
