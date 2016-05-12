using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    internal sealed class KafkaProducerTopicLogger : IKafkaProducerTopicLogger
    {
        [NotNull] private readonly IKafkaProducerLogger _logger;
        [CanBeNull] private IKafkaProducerTopic _topic;

        public KafkaProducerTopicLogger([NotNull] IKafkaProducerLogger logger)
        {
            _logger = logger;
        }

        public void SetTopic(IKafkaProducerTopic topic)
        {
            _topic = topic;
        }
    }

    internal sealed class KafkaProducerTopicLogger<TKey, TData> : IKafkaProducerTopicLogger
    {
        [NotNull] private readonly IKafkaProducerLogger<TKey, TData> _logger;
        [CanBeNull] private IKafkaProducerTopic<TKey, TData> _topic;

        public KafkaProducerTopicLogger([NotNull] IKafkaProducerLogger<TKey, TData> logger)
        {
            _logger = logger;
        }

        public void SetTopic(IKafkaProducerTopic<TKey, TData> topic)
        {
            _topic = topic;
        }
    }
}
