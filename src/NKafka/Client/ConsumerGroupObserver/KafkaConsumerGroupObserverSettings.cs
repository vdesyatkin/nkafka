using JetBrains.Annotations;
using System;

namespace NKafka.Client.ConsumerGroupObserver
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverSettings
    {        
        public readonly TimeSpan OffsetsUpdatePeriod;
        public readonly TimeSpan ErrorRetryPeriod;        

        public KafkaConsumerGroupObserverSettings(TimeSpan offsetUpdatePeriod, TimeSpan errorRetryPeriod)
        {
            OffsetsUpdatePeriod = offsetUpdatePeriod;
            ErrorRetryPeriod = errorRetryPeriod;            
        }
    }

}
