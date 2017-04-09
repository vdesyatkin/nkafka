using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettings
    {
        public readonly TimeSpan JoinGroupServerTimeout;
        public readonly TimeSpan SyncGroupServerTimeout;
        public readonly TimeSpan HeartbeatServerTimeout;
        public readonly TimeSpan OffsetFetchServerTimeout;
        public readonly TimeSpan OffsetCommitServerTimeout;

        public readonly TimeSpan GroupSessionTimeout;
        public readonly TimeSpan GroupRebalanceTimeout;
        public readonly TimeSpan OffsetCommitPeriod;
        public readonly TimeSpan HeartbeatPeriod;
        public readonly TimeSpan OffsetCommitRetentionTime;

        public readonly TimeSpan ErrorRetryPeriod;

        public readonly IReadOnlyList<KafkaConsumerGroupSettingsProtocol> Protocols;
        public readonly string OffsetCommitMetadata;

        public KafkaConsumerGroupSettings(TimeSpan joinGroupServerTimeout, TimeSpan syncGroupServerTimeout,
            TimeSpan heartbeatServerTimeout, TimeSpan offsetFetchServerTimeout, TimeSpan offsetCommitServerTimeout,
            TimeSpan groupSessionTimeout, TimeSpan groupRebalanceTimeout,
            TimeSpan heartbeatPeriod, TimeSpan offsetCommitPeriod,
            TimeSpan offsetCommitRetentionTime,
            TimeSpan errorRetryPeriod,
            IReadOnlyList<KafkaConsumerGroupSettingsProtocol> protocols,
            string offsetCommitMetadata)
        {
            JoinGroupServerTimeout = joinGroupServerTimeout;
            SyncGroupServerTimeout = syncGroupServerTimeout;
            HeartbeatServerTimeout = heartbeatServerTimeout;
            OffsetFetchServerTimeout = offsetFetchServerTimeout;
            OffsetCommitServerTimeout = offsetCommitServerTimeout;
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