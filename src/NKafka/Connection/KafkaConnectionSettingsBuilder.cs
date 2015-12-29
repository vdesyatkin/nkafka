using System;
using JetBrains.Annotations;

namespace NKafka.Connection
{
    public sealed class KafkaConnectionSettingsBuilder
    {
        private TimeSpan? _regularReconnectPeriod;
        private TimeSpan? _errorStateReconnectPeriod;
        private TimeSpan? _heartbeatPeriod;
        private TimeSpan? _transportLatency;

        [PublicAPI]
        public KafkaConnectionSettingsBuilder SetRegularReconnectPeriod(TimeSpan period)
        {
            _regularReconnectPeriod = period;
            return this;
        }

        [PublicAPI]
        public KafkaConnectionSettingsBuilder SetErrorStateReconnectPeriod(TimeSpan period)
        {
            _errorStateReconnectPeriod = period;
            return this;
        }

        [PublicAPI]
        public KafkaConnectionSettingsBuilder SetHeartbeatPeriod(TimeSpan period)
        {
            _heartbeatPeriod = period;
            return this;
        }

        [PublicAPI]
        public KafkaConnectionSettingsBuilder SetTransportLatency(TimeSpan latency)
        {
            _transportLatency = latency;
            return this;
        }

        public KafkaConnectionSettings Build()
        {
            var regularReconnectPeriod = _regularReconnectPeriod ?? TimeSpan.FromMinutes(30);
            var errorStateReconnectPeriod = _errorStateReconnectPeriod ?? TimeSpan.FromSeconds(30);
            var heartbeatPeriod = _heartbeatPeriod ?? TimeSpan.FromSeconds(30);
            var transportLatency = _transportLatency ?? TimeSpan.Zero;

            return new KafkaConnectionSettings(
                regularReconnectPeriod,
                errorStateReconnectPeriod,
                heartbeatPeriod,
                transportLatency);
        }
    }
}
