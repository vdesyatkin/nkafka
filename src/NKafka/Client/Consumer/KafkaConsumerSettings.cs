using System;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaConsumerSettings
    {        
        public readonly int ConsumeBatchMinSizeBytes;
        public readonly TimeSpan ConsumeTimeout;

        public KafkaConsumerSettings(          
          int consumeBatchMinSizeBytes,
          TimeSpan consumeTimeout
          )
        {            
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeTimeout = consumeTimeout;
        }
    }
}
