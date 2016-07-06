using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Logging
{    
    internal sealed class KafkaConsumerTopicBufferLogger<TKey, TData> : IKafkaConsumerTopicBufferLogger
    {
        [NotNull] private readonly IKafkaConsumerLogger<TKey, TData> _logger;
        [CanBeNull] private IKafkaConsumerTopic<TKey, TData> _topic;

        public KafkaConsumerTopicBufferLogger([NotNull] IKafkaConsumerLogger<TKey, TData> logger)
        {
            _logger = logger;
        }

        public void SetTopic([NotNull] IKafkaConsumerTopic<TKey, TData> topic)
        {
            _topic = topic;
        }
        
        public void OnSerializationError(KafkaConsumerTopicSerializationErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;
            try
            {
                _logger.OnSerializationError(topic, error);
            }
            catch (Exception)
            {
                //ignored                
            }
        }
    }
}
