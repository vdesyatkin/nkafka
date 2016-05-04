using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup;
using NKafka.Client.ConsumerGroup.Assignment;
using NKafka.Client.ConsumerGroup.Assignment.Strategies;

namespace NKafka.Client.ConsumerGroupObserver
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverSettingsBuilder
    {        
        private TimeSpan? _offsetUpdatePeriod;
        private TimeSpan? _errorRetryPeriod;
        
        [PublicAPI, NotNull]
        public static KafkaConsumerGroupObserverSettings Default => new KafkaConsumerGroupObserverSettingsBuilder().Build();


        [PublicAPI, NotNull]
        public KafkaConsumerGroupObserverSettingsBuilder SetOffsetUpdatePeriod(TimeSpan period)
        {
            _offsetUpdatePeriod = period;
            return this;
        }       

        [PublicAPI, NotNull]
        public KafkaConsumerGroupObserverSettingsBuilder SetErrorRetryPeriod(TimeSpan period)
        {
            _errorRetryPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaConsumerGroupObserverSettings Build()
        {            
            var offsetUpdatePeriod = _offsetUpdatePeriod ?? TimeSpan.FromSeconds(5);            
            var errorRetryPeriod = _errorRetryPeriod ?? TimeSpan.FromSeconds(10);

            return new KafkaConsumerGroupObserverSettings(offsetUpdatePeriod, errorRetryPeriod);                
        }        
    }
}
