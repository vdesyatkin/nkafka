using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Logging
{
    internal sealed class KafkaCoordinatorGroupLogger : IKafkaCoordinatorGroupLogger
    {
        [NotNull] private readonly IKafkaConsumerGroupLogger _logger;
        [CanBeNull] private IKafkaConsumerGroup _group;

        public KafkaCoordinatorGroupLogger([NotNull] IKafkaConsumerGroupLogger logger)
        {
            _logger = logger;
        }

        public void SetGroup([NotNull] IKafkaConsumerGroup group)
        {
            _group = group;
        }

        public void OnTransportError(KafkaConsumerGroupTransportErrorInfo error)
        {
            var group = _group;
            if (group == null) return;

            try
            {
                _logger.OnTransportError(group, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnAssignmentError(KafkaConsumerGroupAssignmentErrorInfo error)
        {
            var group = _group;
            if (group == null) return;

            try
            {
                _logger.OnAssignmentError(group, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnServerRebalance(KafkaConsumerGroupProtocolErrorInfo error)
        {
            var group = _group;
            if (group == null) return;

            try
            {
                _logger.OnServerRebalance(group, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolError(KafkaConsumerGroupProtocolErrorInfo error)
        {
            var group = _group;
            if (group == null) return;

            try
            {
                _logger.OnProtocolError(group, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolWarning(KafkaConsumerGroupProtocolErrorInfo error)
        {
            var group = _group;
            if (group == null) return;

            try
            {
                _logger.OnProtocolWarning(group, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }
}
