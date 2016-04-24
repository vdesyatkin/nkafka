using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{    
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettings
    {
        public readonly TimeSpan JoinGroupServerWaitTime;
        public readonly TimeSpan SyncGroupServerWaitTime;
        public readonly TimeSpan HeartbeatServerWaitTime;
        public readonly TimeSpan OffsetFetchServerWaitTime;
        public readonly TimeSpan OffsetCommitServerWaitTime;

        public readonly TimeSpan GroupSessionTimeout;
        public readonly TimeSpan OffsetCommitPeriod;
        public readonly TimeSpan HeartbeatPeriod;
        public readonly TimeSpan OffsetCommitRetentionTime;

        public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;
        public readonly string OffsetCommitCustomData;

        public KafkaConsumerGroupSettings(TimeSpan joinGroupServerWaitTime, TimeSpan syncGroupServerWaitTime,
            TimeSpan heartbeatServerWaitTime, TimeSpan offsetFetchServerWaitTime, TimeSpan offsetCommitServerWaitTime,
            TimeSpan groupSessionTimeout, TimeSpan heartbeatPeriod, TimeSpan offsetCommitPeriod,
            TimeSpan offsetCommitRetentionTime, IReadOnlyList<KafkaConsumerGroupSettingsProtocol> protocols,
            string offsetCommitCustomData)
        {
            JoinGroupServerWaitTime = joinGroupServerWaitTime;
            SyncGroupServerWaitTime = syncGroupServerWaitTime;
            HeartbeatServerWaitTime = heartbeatServerWaitTime;
            OffsetFetchServerWaitTime = offsetFetchServerWaitTime;
            OffsetCommitServerWaitTime = offsetCommitServerWaitTime;
            GroupSessionTimeout = groupSessionTimeout;            
            HeartbeatPeriod = heartbeatPeriod;
            OffsetCommitPeriod = offsetCommitPeriod;
            OffsetCommitRetentionTime = offsetCommitRetentionTime;
            Protocols = protocols;
            OffsetCommitCustomData = offsetCommitCustomData;
        }
    }
}
