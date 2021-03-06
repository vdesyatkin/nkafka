﻿using System;
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

        public static readonly TimeSpan DefaultJoinGroupRequestTimeout = TimeSpan.FromMinutes(2);
        public static readonly TimeSpan DefaultSyncGroupRequestTimeout = TimeSpan.FromMinutes(1);
        public static readonly TimeSpan DefaultHeartbeatGroupRequestTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultOffsetFetchRequestTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan DefaultOffsetCommitRequestTimeout = TimeSpan.FromSeconds(30);

        public static readonly TimeSpan DefaultGroupSessionTimeout = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan MinGroupSessionTimeout = TimeSpan.FromSeconds(6);
        public static readonly TimeSpan MaxGroupSessionTimeoutV09 = TimeSpan.FromSeconds(30);
        public static readonly TimeSpan MaxGroupSessionTimeoutV010 = TimeSpan.FromSeconds(300);

        public static readonly TimeSpan DefaultGroupRebalanceTimeout = TimeSpan.FromMinutes(1);
        public static readonly TimeSpan MinGroupRebalanceTimeout = TimeSpan.FromSeconds(6);
        public static readonly TimeSpan MaxGroupRebalanceTimeout = TimeSpan.FromSeconds(300);

        public static readonly TimeSpan DefaultHeartbeatPeriod = TimeSpan.FromSeconds(10);
        public static readonly TimeSpan DefaultOffsetCommitPeriod = TimeSpan.FromSeconds(10);
        public static readonly TimeSpan DefaultOffsetCommitRetentionTime = TimeSpan.FromDays(1);

        public static readonly TimeSpan ErrorRetryPeriod = TimeSpan.FromSeconds(10);

        [NotNull]
        public static readonly KafkaConsumerGroupSettings Default = new KafkaConsumerGroupSettingsBuilder().Build();

        private TimeSpan? _joinGroupTimeout;
        private TimeSpan? _syncGroupTimeout;
        private TimeSpan? _heartbeatTimeout;
        private TimeSpan? _offsetFetchTimeout;
        private TimeSpan? _offsetCommitTimeout;

        /// <summary>
        /// 6-30 seconds by default
        /// </summary>        
        private TimeSpan? _groupSessionTimeout;
        private TimeSpan? _groupRebalanceTimeout;
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

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetJoinGroupTimeout(TimeSpan timeout)
        {
            _joinGroupTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetSyncGroupTimeout(TimeSpan timeout)
        {
            _syncGroupTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetHeartbeatTimeout(TimeSpan timeout)
        {
            _heartbeatTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetFetchTimeout(TimeSpan timeout)
        {
            _offsetFetchTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitTimeout(TimeSpan timeout)
        {
            _offsetCommitTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetGroupTimeout(TimeSpan timeout)
        {
            _groupSessionTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetGroupRebalanceTimeout(TimeSpan timeout)
        {
            _groupRebalanceTimeout = timeout;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetHeartbeatPeriod(TimeSpan period)
        {
            _heartbeatPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitPeriod(TimeSpan period)
        {
            _offsetCommitPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitRetentionTime(TimeSpan retentionTime)
        {
            _offsetCommitRetentionTime = retentionTime;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitMetadata(string metadata)
        {
            _offsetCommitMetadta = metadata;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettingsProtocolBuilder BeginAppendProtocol([NotNull] string protocolName,
            short protocolVersion)
        {
            return new KafkaConsumerSettingsProtocolBuilder(this, protocolName, protocolVersion);
        }

        [NotNull]
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

        [NotNull]
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

        [NotNull]
        public KafkaConsumerGroupSettings Build()
        {
            var joinGroupServerTimeout = _joinGroupTimeout ?? DefaultJoinGroupRequestTimeout;
            var syncGroupServerTimeout = _syncGroupTimeout ?? DefaultSyncGroupRequestTimeout;
            var heartbeatServerTimeout = _heartbeatTimeout ?? DefaultHeartbeatGroupRequestTimeout;
            var offsetFetchServerTimeout = _offsetFetchTimeout ?? DefaultOffsetFetchRequestTimeout;
            var offsetCommitServerTimeout = _offsetCommitTimeout ?? DefaultOffsetCommitRequestTimeout;

            var groupSessionTimeout = _groupSessionTimeout ?? DefaultGroupSessionTimeout;
            var groupRebalanceTimeout = _groupRebalanceTimeout ?? DefaultGroupRebalanceTimeout;
            var heartbeatPeriod = _heartbeatPeriod ?? DefaultHeartbeatGroupRequestTimeout;
            var offsetCommitPeriod = _offsetCommitPeriod ?? DefaultOffsetCommitPeriod;
            var offsetCommitRetentionTime = _offsetCommitRetentionTime ?? DefaultOffsetCommitRetentionTime;

            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            var protocols = _protocols.ToArray();
            if (protocols.Length == 0)
            {
                protocols = new[] { DefaultProtocol };
            }

            return new KafkaConsumerGroupSettings(
                joinGroupServerTimeout,
                syncGroupServerTimeout,
                heartbeatServerTimeout,
                offsetFetchServerTimeout,
                offsetCommitServerTimeout,
                groupSessionTimeout,
                groupRebalanceTimeout,
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

            [NotNull]
            public KafkaConsumerSettingsProtocolBuilder SetCustomData([NotNull] byte[] customData)
            {
                _customData = customData;
                return this;
            }

            [NotNull]
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

            [NotNull]
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

            [NotNull]
            public KafkaConsumerGroupSettingsBuilder EndAppendProtocol()
            {
                var protocol = new KafkaConsumerGroupSettingsProtocol(_protocolName, _protocolVersion, _strategies.ToArray(), _customData);
                return _baseBuilder.AppendProtocol(protocol);
            }
        }
    }
}