using System;

namespace NKafka.Connection
{
    public sealed class KafkaConnectionSettings
    {        
        public readonly TimeSpan RegularReconnectPeriod;
        public readonly TimeSpan ErrorStateReconnectPeriod;
        public readonly TimeSpan HeartbeatPeriod;
        public readonly TimeSpan TransportLatency;

        public KafkaConnectionSettings(TimeSpan regularReconnectPeriod, TimeSpan errorStateReconnectPeriod, TimeSpan heartbeatPeriod, TimeSpan transportLatency)
        {
            TransportLatency = transportLatency;
            RegularReconnectPeriod = regularReconnectPeriod;
            ErrorStateReconnectPeriod = errorStateReconnectPeriod;
            HeartbeatPeriod = heartbeatPeriod;
        }        
    }
}
