using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Logging
{
    internal sealed class KafkaConsumerTopicLogger : IKafkaConsumerTopicLogger
    {
        [NotNull] private readonly IKafkaConsumerLogger _logger;
        [CanBeNull] private IKafkaConsumerTopic _topic;

        public KafkaConsumerTopicLogger([NotNull] IKafkaConsumerLogger logger)
        {
            _logger = logger;
        }

        public void SetTopic([NotNull] IKafkaConsumerTopic topic)
        {
            _topic = topic;
        }

        public void OnTransportError(KafkaConsumerTopicTransportErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnTransportError(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnServerRebalance(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnServerRebalance(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolError(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnProtocolError(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolWarning(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnProtocolWarning(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnErrorReset()
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnErrorReset(topic);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }

    internal sealed class KafkaConsumerTopicLogger<TKey, TData> : IKafkaConsumerTopicLogger
    {
        [NotNull]
        private readonly IKafkaConsumerLogger<TKey, TData> _logger;
        [CanBeNull]
        private IKafkaConsumerTopic<TKey, TData> _topic;

        public KafkaConsumerTopicLogger([NotNull] IKafkaConsumerLogger<TKey, TData> logger)
        {
            _logger = logger;
        }

        public void SetTopic([NotNull] IKafkaConsumerTopic<TKey, TData> topic)
        {
            _topic = topic;
        }

        public void OnTransportError(KafkaConsumerTopicTransportErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnTransportError(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnServerRebalance(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnServerRebalance(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolError(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnProtocolError(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolWarning(KafkaConsumerTopicProtocolErrorInfo error)
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnProtocolWarning(topic, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnErrorReset()
        {
            var topic = _topic;
            if (topic == null) return;

            try
            {
                _logger.OnErrorReset(topic);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }
}
