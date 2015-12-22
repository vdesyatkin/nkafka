using System;

namespace NKafka.Connection
{
    public class KafkaConnectionSettings
    {
        public readonly TimeSpan RegularReconnectPeriod;
        public readonly TimeSpan ErrorStateReconnectPeriod;
        public readonly TimeSpan HeartbeatPeriod;        

        public KafkaConnectionSettings(TimeSpan regularReconnectPeriod, TimeSpan errorStateReconnectPeriod, TimeSpan heartbeatPeriod)
        {
            RegularReconnectPeriod = regularReconnectPeriod;
            ErrorStateReconnectPeriod = errorStateReconnectPeriod;
            HeartbeatPeriod = heartbeatPeriod;            
        }        
    }
}
