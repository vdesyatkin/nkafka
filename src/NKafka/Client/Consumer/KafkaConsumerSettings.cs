using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettings
    {        
        public readonly int ConsumeBatchMinSizeBytes;
        public readonly int ConsumeBatchMaxSizeBytes;
        public readonly TimeSpan ConsumeServerWaitTime;

        public KafkaConsumerSettings(          
          int consumeBatchMinSizeBytes,
          int consumeBatchMaxSizeBytes,
          TimeSpan consumeServerWaitTime
          )
        {            
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeBatchMaxSizeBytes = consumeBatchMaxSizeBytes;
            ConsumeServerWaitTime = consumeServerWaitTime;
        }
    }
}
