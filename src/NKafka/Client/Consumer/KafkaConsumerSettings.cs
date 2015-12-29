using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
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
