using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroups
{    
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettings
    {
        public readonly TimeSpan GroupSessionTimeout;
        public readonly IReadOnlyList<KafkaConsumerGroupProtocolInfo> Protocols;        

        public KafkaConsumerGroupSettings(
          TimeSpan groupSessionTimeout,
          [NotNull] IReadOnlyList<KafkaConsumerGroupProtocolInfo> protocols)          
        {
            GroupSessionTimeout = groupSessionTimeout;
            Protocols = protocols;
        }
    }
}
