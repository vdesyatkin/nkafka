using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Assignment.Strategies;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSettingsBuilder
    {
        // https://kafka.apache.org/documentation.html#brokerconfigs

        [PublicAPI, NotNull]
        public readonly static KafkaConsumerGroupSettings Default = new KafkaConsumerGroupSettingsBuilder().Build();
        
        public readonly static TimeSpan DefaultJoinGroupRequestServerTimeout = TimeSpan.FromMinutes(2);
        public readonly static TimeSpan DefaultSyncGroupRequestServerTimeout = TimeSpan.FromMinutes(1);
        public readonly static TimeSpan DefaultHeartbeatGroupRequestServerTimeout = TimeSpan.FromSeconds(5);
        public readonly static TimeSpan DefaultOffsetFetchRequestServerTimeout = TimeSpan.FromSeconds(5);
        public readonly static TimeSpan DefaultOffsetCommitRequestServerTimeout = TimeSpan.FromSeconds(10);

        public readonly static TimeSpan DefaultGroupSessionTimeout = TimeSpan.FromSeconds(30);
        public readonly static TimeSpan MinGroupSessionTimeout = TimeSpan.FromSeconds(6);
        public readonly static TimeSpan MaxGroupSessionTimeout = TimeSpan.FromSeconds(30);

        public readonly static TimeSpan DefaultHeartbeatPeriod = TimeSpan.FromSeconds(3);
        public readonly static TimeSpan DefaultOffsetCommitPeriod = TimeSpan.FromSeconds(60);
        public readonly static TimeSpan DefaultOffsetCommitRetentionTime = TimeSpan.FromDays(1);

        public readonly static TimeSpan ErrorRetryPeriod = TimeSpan.FromSeconds(10);

        private TimeSpan? _joinGroupServerWaitTime;
        private TimeSpan? _syncGroupServerWaitTime;
        private TimeSpan? _heartbeatServerWaitTime;
        private TimeSpan? _offsetFetchServerWaitTime;
        private TimeSpan? _offsetCommitServerWaitTime;

        /// <summary>
        /// 6-30 seconds by default
        /// </summary>        
        private TimeSpan? _groupSessionTimeout;
        private TimeSpan? _heartbeatPeriod;
        private TimeSpan? _offsetCommitPeriod;
        private TimeSpan? _offsetCommitRetentionTime;

        private TimeSpan? _errorRetryPeriod;

        [NotNull]
        private List<KafkaConsumerGroupSettingsProtocol> _protocols;
        private string _offsetCommitMetadta;        

        public static readonly KafkaConsumerAssignmentStrategyInfo DefaultStrategy =
            new KafkaConsumerAssignmentStrategyInfo("round_robin", new KafkaConsumerAssignmentRoundRobinStrategy());

        public static readonly KafkaConsumerGroupSettingsProtocol DefaultProtocol =
                new KafkaConsumerGroupSettingsProtocol("nkafka_default", 1, new[] { DefaultStrategy }, null);

        public KafkaConsumerGroupSettingsBuilder()
        {
            _protocols = new List<KafkaConsumerGroupSettingsProtocol>();
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetJoinGroupServerTimeout(TimeSpan timeout)
        {
            _joinGroupServerWaitTime = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetSyncGroupServerTimeout(TimeSpan timeout)
        {
            _syncGroupServerWaitTime = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetHeartbeatServerTimeout(TimeSpan timeout)
        {
            _heartbeatServerWaitTime = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetFetchServerTimeout(TimeSpan timeout)
        {
            _offsetFetchServerWaitTime = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitServerTimeout(TimeSpan timeout)
        {
            _offsetCommitServerWaitTime = timeout;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetGroupSessionLifetime(TimeSpan lifeTime)
        {
            _groupSessionTimeout = lifeTime;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetHeartbeatPeriod(TimeSpan period)
        {
            _heartbeatPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitPeriod(TimeSpan period)
        {
            _offsetCommitPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitRetentionTime(TimeSpan retentionTime)
        {
            _offsetCommitRetentionTime = retentionTime;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitMetadata(string metadata)
        {
            _offsetCommitMetadta = metadata;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerSettingsProtocolBuilder BeginAppendProtocol([NotNull] string protocolName,
            short protocolVersion)
        {
            return new KafkaConsumerSettingsProtocolBuilder(this, protocolName, protocolVersion);
        }

        public KafkaConsumerGroupSettingsBuilder AppendProtocol(
            [NotNull] string protocolName,
            short protocolVersion,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerAssignmentStrategyInfo> assignmentStrategies,
            [CanBeNull] byte[] customData = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable HeuristicUnreachableCode            
            if (string.IsNullOrEmpty(protocolName)) return this;
            if (assignmentStrategies == null) return this;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore HeuristicUnreachableCode            
            var protocol = new KafkaConsumerGroupSettingsProtocol(protocolName, protocolVersion, assignmentStrategies, customData);
            return AppendProtocol(protocol);
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettingsBuilder AppendProtocol([NotNull] KafkaConsumerGroupSettingsProtocol protocol)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable HeuristicUnreachableCode    
            if (protocol == null) return this;
            // ReSharper restore HeuristicUnreachableCode
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            _protocols.Add(protocol);
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupSettings Build()
        {
            var joinGroupServerWaitTime = _joinGroupServerWaitTime ?? DefaultJoinGroupRequestServerTimeout;
            var syncGroupServerWaitTime = _syncGroupServerWaitTime ?? DefaultSyncGroupRequestServerTimeout;
            var heartbeatServerWaitTime = _heartbeatServerWaitTime ?? DefaultHeartbeatGroupRequestServerTimeout;
            var offsetFetchServerWaitTime = _offsetFetchServerWaitTime ?? DefaultOffsetFetchRequestServerTimeout;
            var offsetCommitServerWaitTime = _offsetCommitServerWaitTime ?? DefaultOffsetCommitRequestServerTimeout;

            var groupSessionTimeout = _groupSessionTimeout ?? DefaultGroupSessionTimeout;
            var heartbeatPeriod = _heartbeatPeriod ?? DefaultHeartbeatGroupRequestServerTimeout;
            var offsetCommitPeriod = _offsetCommitPeriod ?? DefaultOffsetCommitRequestServerTimeout;
            var offsetCommitRetentionTime = _offsetCommitRetentionTime ?? DefaultOffsetCommitRetentionTime;

            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            var protocols = _protocols.ToArray();
            if (protocols.Length == 0)
            {
                protocols = new[] { DefaultProtocol };
            }

            return new KafkaConsumerGroupSettings(
                joinGroupServerWaitTime,
                syncGroupServerWaitTime,
                heartbeatServerWaitTime,
                offsetFetchServerWaitTime,
                offsetCommitServerWaitTime,
                groupSessionTimeout,
                heartbeatPeriod,
                offsetCommitPeriod,
                offsetCommitRetentionTime,
                errorRetryPeriod,
                protocols,
                _offsetCommitMetadta);
        }

        [PublicAPI]
        public sealed class KafkaConsumerSettingsProtocolBuilder
        {
            [NotNull]
            private readonly KafkaConsumerGroupSettingsBuilder _baseBuilder;

            [NotNull]
            private readonly string _protocolName;

            private readonly short _protocolVersion;

            [NotNull]
            private readonly List<KafkaConsumerAssignmentStrategyInfo> _strategies;

            [CanBeNull]
            private byte[] _customData;

            internal KafkaConsumerSettingsProtocolBuilder(
                [NotNull] KafkaConsumerGroupSettingsBuilder baseBuilder,
                [NotNull] string protocolName,
                short protocolVersion)
            {
                _baseBuilder = baseBuilder;
                _protocolName = protocolName;
                _protocolVersion = protocolVersion;
                _strategies = new List<KafkaConsumerAssignmentStrategyInfo>();
            }

            public KafkaConsumerSettingsProtocolBuilder SetCustomData([NotNull] byte[] customData)
            {
                _customData = customData;
                return this;
            }

            public KafkaConsumerSettingsProtocolBuilder AppendStrategy([NotNull]string strategyName, [NotNull]IKafkaConsumerAssignmentStrategy strategy)
            {
                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                // ReSharper disable HeuristicUnreachableCode
                if (strategy == null) return this;
                if (string.IsNullOrEmpty(strategyName)) return this;
                // ReSharper restore ConditionIsAlwaysTrueOrFalse
                // ReSharper restore HeuristicUnreachableCode

                var strategyInfo = new KafkaConsumerAssignmentStrategyInfo(strategyName, strategy);
                return AppendStrategy(strategyInfo);
            }

            public KafkaConsumerSettingsProtocolBuilder AppendStrategy([NotNull]KafkaConsumerAssignmentStrategyInfo strategyInfo)
            {
                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                // ReSharper disable HeuristicUnreachableCode
                if (strategyInfo == null) return this;
                // ReSharper restore ConditionIsAlwaysTrueOrFalse
                // ReSharper restore HeuristicUnreachableCode

                _strategies.Add(strategyInfo);
                return this;
            }

            public KafkaConsumerGroupSettingsBuilder EndAppendProtocol()
            {
                var protocol = new KafkaConsumerGroupSettingsProtocol(_protocolName, _protocolVersion, _strategies.ToArray(), _customData);
                return _baseBuilder.AppendProtocol(protocol);
            }
        }
    }
}