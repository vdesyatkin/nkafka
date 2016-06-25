using System;
using JetBrains.Annotations;

namespace NKafka.Connection
{
    [PublicAPI]
    public sealed class KafkaConnectionSettingsBuilder
    {        
        public readonly TimeSpan DefaultRegularReconnectionPeriod = TimeSpan.FromMinutes(30);
        public readonly TimeSpan DefaultErrorStateReconnectPeriod = TimeSpan.FromSeconds(15);
        public readonly TimeSpan DefaultHeartbeatPeriod = TimeSpan.FromSeconds(30);
        public readonly TimeSpan DefaultTransportLatency = TimeSpan.FromSeconds(1);

        [NotNull] public static KafkaConnectionSettings Default = new KafkaConnectionSettingsBuilder().Build();

        private TimeSpan? _regularReconnectPeriod;
        private TimeSpan? _errorStateReconnectPeriod;
        private TimeSpan? _heartbeatPeriod;
        private TimeSpan? _transportLatency;        

        [NotNull]
        public KafkaConnectionSettingsBuilder SetRegularReconnectPeriod(TimeSpan period)
        {
            _regularReconnectPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConnectionSettingsBuilder SetErrorStateReconnectPeriod(TimeSpan period)
        {
            _errorStateReconnectPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConnectionSettingsBuilder SetHeartbeatPeriod(TimeSpan period)
        {
            _heartbeatPeriod = period;
            return this;
        }

        [NotNull]
        public KafkaConnectionSettingsBuilder SetTransportLatency(TimeSpan latency)
        {
            _transportLatency = latency;
            return this;
        }

        [NotNull]
        public KafkaConnectionSettings Build()
        {
            var regularReconnectPeriod = _regularReconnectPeriod ?? DefaultRegularReconnectionPeriod;
            var errorStateReconnectPeriod = _errorStateReconnectPeriod ?? DefaultErrorStateReconnectPeriod;
            var heartbeatPeriod = _heartbeatPeriod ?? DefaultHeartbeatPeriod;
            var transportLatency = _transportLatency ?? DefaultTransportLatency;

            return new KafkaConnectionSettings(
                regularReconnectPeriod,
                errorStateReconnectPeriod,
                heartbeatPeriod,
                transportLatency);
        }
    }
}
