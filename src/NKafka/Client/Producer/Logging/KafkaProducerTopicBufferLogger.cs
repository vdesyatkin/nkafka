using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    internal sealed class KafkaProducerTopicBufferLogger: IKafkaProducerTopicBufferLogger
    {
        [NotNull] private readonly IKafkaProducerLogger _logger;
        [CanBeNull] private IKafkaProducerTopic _topic;

        public KafkaProducerTopicBufferLogger([NotNull] IKafkaProducerLogger logger)
        {
            _logger = logger;
        }

        public void SetTopic([NotNull] IKafkaProducerTopic topic)
        {
            _topic = topic;
        }

        public void OnPartitioningError(KafkaProducerTopicPartitioningErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;
            try
            {
                _logger.OnPartitioningError(topic, error);
            }
            catch (Exception)
            {
                //ignored                
            }
        }
    }

    internal sealed class KafkaProducerTopicBufferLogger<TKey, TData> : IKafkaProducerTopicBufferLogger<TKey, TData>
    {
        [NotNull] private IKafkaProducerLogger<TKey, TData> _logger;
        [CanBeNull] private IKafkaProducerTopic<TKey, TData> _topic;

        public KafkaProducerTopicBufferLogger([NotNull] IKafkaProducerLogger<TKey, TData> logger)
        {
            _logger = logger;
        }

        public void SetTopic([NotNull] IKafkaProducerTopic<TKey, TData> topic)
        {
            _topic = topic;
        }

        public void OnPartitioningError(KafkaProducerTopicPartitioningErrorInfo<TKey, TData> error)
        {
            var topic = _topic;
            if (topic == null) return;
            try
            {
                _logger.OnPartitioningError(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnSerializationError(KafkaProducerTopicSerializationErrorInfo<TKey, TData> error)
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
