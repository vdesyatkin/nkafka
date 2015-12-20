using System;

namespace NKafka.DevConsole.DevProtocol
{
    public struct KafkaTimeout
    {
        public TimeSpan TimeoutPeriod { get; private set; }
        public int TimeoutMs { get; private set; }

        public KafkaTimeout(TimeSpan period)
        {
            TimeoutPeriod = period;
            TimeoutMs = (int)Math.Round(period.TotalMilliseconds);                
        }
    }
}
