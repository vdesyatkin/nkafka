using System;

namespace NKafka.Client.Consumer
{
    public sealed class KafkaConsumerSettings
    {        
        public readonly TimeSpan ConsumePeriod;
        public readonly int ConsumeBatchMinSizeBytes;
        public readonly TimeSpan ConsumeTimeout;

        public KafkaConsumerSettings(          
          TimeSpan consumePeriod,
          int consumeBatchMinSizeBytes,
          TimeSpan consumeTimeout
          )
        {            
            ConsumePeriod = consumePeriod;
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeTimeout = consumeTimeout;
        }
    }
}
