using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettings
    {        
        public readonly int ConsumeBatchMinSizeBytes;
        public readonly int ConsumeBatchMaxSizeBytes;
        public readonly TimeSpan ConsumeServerWaitTime;        
        public readonly IReadOnlyList<KafkaConsumerProtocolInfo> Protocols;
        public readonly TimeSpan GroupSessionTimeout;

        public KafkaConsumerSettings(          
          int consumeBatchMinSizeBytes,
          int consumeBatchMaxSizeBytes,
          TimeSpan consumeServerWaitTime,          
          [NotNull] IReadOnlyList<KafkaConsumerProtocolInfo> protocols,
          TimeSpan groupSessionTimeout
          )
        {            
            ConsumeBatchMinSizeBytes = consumeBatchMinSizeBytes;
            ConsumeBatchMaxSizeBytes = consumeBatchMaxSizeBytes;
            ConsumeServerWaitTime = consumeServerWaitTime;
            Protocols = protocols;
            GroupSessionTimeout = groupSessionTimeout;
        }
    }
}
