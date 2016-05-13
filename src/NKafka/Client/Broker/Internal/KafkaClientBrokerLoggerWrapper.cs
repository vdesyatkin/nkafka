using System;
using JetBrains.Annotations;
using NKafka.Client.Broker.Diagnostics;
using NKafka.Connection.Logging;

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

        public void OnConnected()
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

        public void OnDisconnected()
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

        public void OnConnectionError(KafkaBrokerConnectionErrorInfo error)
        {
            try
            {
                _logger.OnBrokerConnectionError(_broker, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void OnProtocolError(KafkaBrokerProtocolErrorInfo error)
        {
            try
            {
                _logger.OnBrokerProtocolError(_broker, error);
            }
            catch (Exception)
            {
                //ignored
            }
        }        
    }
}
