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
        /// <summary>
        /// 6-30 seconds by default
        /// </summary>        
        private TimeSpan? _joinGroupServerWaitTime;
        private TimeSpan? _syncGroupServerWaitTime;
        private TimeSpan? _heartbeatServerWaitTime;
        private TimeSpan? _offsetFetchServerWaitTime;
        private TimeSpan? _offsetCommitServerWaitTime;

        private TimeSpan? _groupSessionTimeout;
        private TimeSpan? _heartbeatPeriod;
        private TimeSpan? _offsetCommitPeriod;
        private TimeSpan? _offsetCommitRetentionTime;

        private List<KafkaConsumerGroupProtocolInfo> _protocols;
        private string _offsetCommitCustomData;

        public static KafkaConsumerGroupSettings Default => new KafkaConsumerGroupSettingsBuilder().Build();

        public static readonly KafkaConsumerAssignmentStrategyInfo DefaultStrategy =
            new KafkaConsumerAssignmentStrategyInfo("round_robin", new KafkaConsumerAssignmentRoundRobinStrategy());

        public static readonly KafkaConsumerGroupProtocolInfo DefaultProtocol =
                new KafkaConsumerGroupProtocolInfo("nkafka_default", 1, new[] { DefaultStrategy }, null);        

        public KafkaConsumerGroupSettingsBuilder()
        {
            _protocols = new List<KafkaConsumerGroupProtocolInfo>();
        }               

        public KafkaConsumerGroupSettingsBuilder SetJoinGroupServerWaitTime(TimeSpan waitTime)
        {
            _joinGroupServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetSyncGroupServerWaitTime(TimeSpan waitTime)
        {
            _syncGroupServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetHeartbeatServerWaitTime(TimeSpan waitTime)
        {
            _heartbeatServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetOffsetFetchServerWaitTime(TimeSpan waitTime)
        {
            _offsetFetchServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitServerWaitTime(TimeSpan waitTime)
        {
            _offsetCommitServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetGroupSessionTimeout(TimeSpan timeout)
        {
            _groupSessionTimeout = timeout;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetHeartbeatPeriod(TimeSpan period)
        {
            _heartbeatPeriod = period;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitPeriod(TimeSpan period)
        {
            _offsetCommitPeriod = period;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitRetentionTime(TimeSpan period)
        {
            _offsetCommitRetentionTime = period;
            return this;
        }

        public KafkaConsumerGroupSettingsBuilder SetOffsetCommitCustomData(string customData)
        {
            _offsetCommitCustomData = customData;
            return this;
        }

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
            var protocol = new KafkaConsumerGroupProtocolInfo(protocolName, protocolVersion, assignmentStrategies, customData);
            return AppendProtocol(protocol);
        }

        public KafkaConsumerGroupSettingsBuilder AppendProtocol([NotNull] KafkaConsumerGroupProtocolInfo protocol)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable HeuristicUnreachableCode    
            if (protocol == null) return this;
            // ReSharper restore HeuristicUnreachableCode
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            _protocols.Add(protocol);
            return this;
        }

        public KafkaConsumerGroupSettings Build()
        {
            var joinGroupServerWaitTime = _joinGroupServerWaitTime ?? TimeSpan.FromMinutes(2);
            var syncGroupServerWaitTime = _joinGroupServerWaitTime ?? TimeSpan.FromMinutes(1);
            var heartbeatServerWaitTime = _heartbeatServerWaitTime ?? TimeSpan.FromSeconds(5);
            var offsetFetchServerWaitTime = _offsetFetchServerWaitTime ?? TimeSpan.FromSeconds(5);
            var offsetCommitServerWaitTime = _offsetCommitServerWaitTime ?? TimeSpan.FromSeconds(10);

            var groupSessionTimeout = _groupSessionTimeout ?? TimeSpan.FromSeconds(30);
            var heartbeatPeriod = _heartbeatPeriod ?? TimeSpan.FromMinutes(1);
            var offsetCommitPeriod = _offsetCommitPeriod ?? TimeSpan.FromMinutes(1);
            var offsetCommitRetentionTime = _offsetCommitRetentionTime ?? TimeSpan.FromDays(7);

            var protocols = _protocols.ToArray();
            if (protocols.Length == 0)
            {
                protocols = new[] {DefaultProtocol};
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
                protocols,
                _offsetCommitCustomData);
        }

        [PublicAPI]
        public sealed class KafkaConsumerSettingsProtocolBuilder
        {
            [NotNull] private readonly KafkaConsumerGroupSettingsBuilder _baseBuilder;

            [NotNull] private readonly string _protocolName;

            private readonly short _protocolVersion;

            [NotNull] private readonly List<KafkaConsumerAssignmentStrategyInfo> _strategies;

            [CanBeNull] private byte[] _customData;            

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
                var protocol = new KafkaConsumerGroupProtocolInfo(_protocolName, _protocolVersion, _strategies.ToArray(), _customData);
                return _baseBuilder.AppendProtocol(protocol);
            }
        }
    }
}
