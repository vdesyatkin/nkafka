using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{    
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettings
    {
        public readonly TimeSpan GroupInitiationServerWaitTime;
        public readonly TimeSpan HeartbeatServerWaitTime;
        public readonly TimeSpan OffsetFetchServerWaitTime;
        public readonly TimeSpan GroupSessionTimeout;
        public readonly IReadOnlyList<KafkaConsumerGroupProtocolInfo> Protocols;        

        public KafkaConsumerGroupSettings(
            TimeSpan groupInitiationServerWaitTime,
            TimeSpan heartbeatServerWaitTime,
            TimeSpan offsetFetchServerWaitTime,
            TimeSpan groupSessionTimeout,
            [NotNull] IReadOnlyList<KafkaConsumerGroupProtocolInfo> protocols)
        {
            GroupInitiationServerWaitTime = groupInitiationServerWaitTime;            
            HeartbeatServerWaitTime = heartbeatServerWaitTime;
            OffsetFetchServerWaitTime = offsetFetchServerWaitTime;
            GroupSessionTimeout = groupSessionTimeout;
            Protocols = protocols;
        }
    }
}
