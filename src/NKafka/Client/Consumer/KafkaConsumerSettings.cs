using System;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaConsumerSettings
    {        
        public readonly int ConsumeBatchMinSizeBytes;
        public readonly TimeSpan ConsumeServerTimeout;

        public KafkaConsumerSettings(          
          int consumeBatchMinSizeBytes,
          TimeSpan consumeServerTimeout
          )
        {            
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeServerTimeout = consumeServerTimeout;
        }
    }
}
