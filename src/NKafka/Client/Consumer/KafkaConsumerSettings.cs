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
        public readonly int? BufferMaxMessageCount;
        public readonly int? BufferedMaxSizeBytes;
        public readonly TimeSpan ErrorRetryPeriod;

        public KafkaConsumerSettings(          
          int consumeBatchMinSizeBytes,
          int consumeBatchMaxSizeBytes,
          TimeSpan consumeServerWaitTime,
          int? bufferMaxMesageCount,
          int? bufferedMaxSizeBytes,
          TimeSpan errorRetryPeriod
          )
        {            
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeBatchMaxSizeBytes = consumeBatchMaxSizeBytes;
            ConsumeServerWaitTime = consumeServerWaitTime;
            BufferMaxMessageCount = bufferMaxMesageCount;
            BufferedMaxSizeBytes = bufferedMaxSizeBytes;
            ErrorRetryPeriod = errorRetryPeriod;
        }
    }
}
