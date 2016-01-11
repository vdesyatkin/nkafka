using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerSettingsBuilder
    {        
        private int? _consumeBatchMinSizeBytes;
        private int? _consumeBatchMaxSizeBytes;
        private TimeSpan? _consumeServerWaitTime;

        /// <summary>
        /// 6-30 seconds by default
        /// </summary>
        private TimeSpan? _groupSessionTimeout;

        private List<KafkaConsumerProtocolInfo> _protocols;

        public KafkaConsumerSettingsBuilder()
        {            
            _protocols = new List<KafkaConsumerProtocolInfo>();
        }
        
        public KafkaConsumerSettingsBuilder SetBatchMinSizeBytes(int batchMinSizeBytes)
        {
            _consumeBatchMinSizeBytes = batchMinSizeBytes;
            return this;
        }

        public KafkaConsumerSettingsBuilder SetBatchMaxSizeBytes(int batchMaxSizeBytes)
        {
            _consumeBatchMaxSizeBytes = batchMaxSizeBytes;
            return this;
        }
        
        public KafkaConsumerSettingsBuilder SetConsumeServerWaitTime(TimeSpan waitTime)
        {
            _consumeServerWaitTime = waitTime;
            return this;
        }

        public KafkaConsumerSettingsBuilder SetGroupSessionTimeout(TimeSpan timeout)
        {
            _groupSessionTimeout = timeout;
            return this;
        }

        public KafkaConsumerSettingsProtocolBuilder BeginAppendProtocol([NotNull] string protocolName,
            short protocolVersion)
        {
            return new KafkaConsumerSettingsProtocolBuilder(this, protocolName, protocolVersion);
        }

        public KafkaConsumerSettingsBuilder AppendProtocol(
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
            if (string.IsNullOrEmpty(protocolName)) return this;
            var protocol = new KafkaConsumerProtocolInfo(protocolName, protocolVersion, assignmentStrategies, customData);
            return AppendProtocol(protocol);
        }

        public KafkaConsumerSettingsBuilder AppendProtocol([NotNull] KafkaConsumerProtocolInfo protocol)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable HeuristicUnreachableCode    
            if (protocol == null) return this;
            // ReSharper restore HeuristicUnreachableCode
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            _protocols.Add(protocol);
            return this;
        }       

        public KafkaConsumerSettings Build()
        {            
            var batchMinSizeBytes = _consumeBatchMinSizeBytes ?? 0;
            var batchMaxSizeBytes = _consumeBatchMaxSizeBytes ?? 200 * 200;
            var consumerServerWaitTime = _consumeServerWaitTime ?? TimeSpan.Zero;
            var groupSessionTimeout = _groupSessionTimeout ?? TimeSpan.FromSeconds(30);
            var protocols = _protocols.ToArray();

            return new KafkaConsumerSettings(                
                batchMinSizeBytes,
                batchMaxSizeBytes,
                consumerServerWaitTime,
                protocols,
                groupSessionTimeout);
        }

        [PublicAPI]
        public sealed class KafkaConsumerSettingsProtocolBuilder
        {
            [NotNull] private readonly KafkaConsumerSettingsBuilder _baseBuilder;

            [NotNull] private readonly string _protocolName;

            private readonly short _protocolVersion;

            [NotNull] private readonly List<KafkaConsumerAssignmentStrategyInfo> _strategies;

            [CanBeNull] private byte[] _customData;

            internal KafkaConsumerSettingsProtocolBuilder(
                [NotNull] KafkaConsumerSettingsBuilder baseBuilder,
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

            public KafkaConsumerSettingsBuilder EndAppendProtocol()
            {
                var protocol = new KafkaConsumerProtocolInfo(_protocolName, _protocolVersion, _strategies.ToArray(), _customData);
                return _baseBuilder.AppendProtocol(protocol);
            }
        }
    }
}
