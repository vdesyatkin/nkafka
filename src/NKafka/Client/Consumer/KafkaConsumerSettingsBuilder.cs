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

        public KafkaConsumerSettingsBuilder SetBatchMinSizeBytes(int batchMinSizeBytes)
        {
            _consumeBatchMinSizeBytes = batchMinSizeBytes;
            return this;
        }

        public KafkaConsumerSettingsBuilder SetBatchMaxSizeBytes(int batchMaxSizeBytes)
        {
            _consumeBatchMaxSizeBytes = batchMaxSizeBytes;
            return this;
        }
        
        public KafkaConsumerSettingsBuilder SetConsumeServerWaitTime(TimeSpan waitTime)
        {
            _consumeServerWaitTime = waitTime;
            return this;
        }
       
        public KafkaConsumerSettings Build()
        {            
            var batchMinSizeBytes = _consumeBatchMinSizeBytes ?? 0;
            var batchMaxSizeBytes = _consumeBatchMaxSizeBytes ?? 200 * 200;
            var consumerServerWaitTime = _consumeServerWaitTime ?? TimeSpan.Zero;            

            return new KafkaConsumerSettings(                
                batchMinSizeBytes,
                batchMaxSizeBytes,
                consumerServerWaitTime);
        }        
    }
}
