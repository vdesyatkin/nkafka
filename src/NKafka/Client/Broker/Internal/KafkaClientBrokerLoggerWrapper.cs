using System;
using JetBrains.Annotations;
using NKafka.Client.Broker.Diagnostics;
using NKafka.Connection.Diagnostics;

namespace NKafka.Client.Broker.Internal
{
    internal sealed class KafkaClientBrokerLoggerWrapper : IKafkaBrokerLogger
    {
        [NotNull] private readonly IKafkaClientBroker _broker;
        [NotNull] private readonly IKafkaClientBrokerLogger _logger;

        public KafkaClientBrokerLoggerWrapper([NotNull] IKafkaClientBroker broker, [NotNull] IKafkaClientBrokerLogger logger)
        {
            _broker = broker;
            _logger = logger;
        }

        public void OnBrokerConnected()
        {
            try
            {
                _logger.OnBrokerConnected(_broker);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnBrokerDisconnected()
        {
            try
            {
                _logger.OnBrokerDisconnected(_broker);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnBrokerError(KafkaBrokerErrorInfo error)
        {
            try
            {
                _logger.OnBrokerError(_broker, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnBrokerRequestError(KafkaBrokerErrorInfo error, KafkaBrokerRequestInfo requestInfo)
        {
            try
            {
                _logger.OnBrokerRequestError(_broker, error, requestInfo);
            }
            catch (Exception)
            {
                //ignored
            }
        }        
    }
}
