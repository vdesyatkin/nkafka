using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettings
    {
        public readonly TimeSpan JoinGroupTimeout;
        public readonly TimeSpan SyncGroupTimeout;
        public readonly TimeSpan HeartbeatTimeout;
        public readonly TimeSpan OffsetFetchTimeout;
        public readonly TimeSpan OffsetCommitTimeout;

        public readonly TimeSpan GroupSessionTimeout;
        public readonly TimeSpan GroupRebalanceTimeout;
        public readonly TimeSpan OffsetCommitPeriod;
        public readonly TimeSpan HeartbeatPeriod;
        public readonly TimeSpan OffsetCommitRetentionTime;

        public readonly TimeSpan ErrorRetryPeriod;

        public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;
        public readonly string OffsetCommitMetadata;

        public KafkaConsumerGroupSettings(TimeSpan joinGroupTimeout, TimeSpan syncGroupTimeout,
            TimeSpan heartbeatTimeout, TimeSpan offsetFetchTimeout, TimeSpan offsetCommitTimeout,
            TimeSpan groupSessionTimeout, TimeSpan groupRebalanceTimeout,
            TimeSpan heartbeatPeriod, TimeSpan offsetCommitPeriod,
            TimeSpan offsetCommitRetentionTime,
            TimeSpan errorRetryPeriod,
            IReadOnlyList<KafkaConsumerGroupSettingsProtocol> protocols,
            string offsetCommitMetadata)
        {
            JoinGroupTimeout = joinGroupTimeout;
            SyncGroupTimeout = syncGroupTimeout;
            HeartbeatTimeout = heartbeatTimeout;
            OffsetFetchTimeout = offsetFetchTimeout;
            OffsetCommitTimeout = offsetCommitTimeout;
            GroupSessionTimeout = groupSessionTimeout;
            GroupRebalanceTimeout = groupRebalanceTimeout;
            HeartbeatPeriod = heartbeatPeriod;
            OffsetCommitPeriod = offsetCommitPeriod;
            OffsetCommitRetentionTime = offsetCommitRetentionTime;
            ErrorRetryPeriod = errorRetryPeriod;
            Protocols = protocols;
            OffsetCommitMetadata = offsetCommitMetadata;
        }
    }
}