using System;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Connection.Logging;

namespace NKafka.Client.Broker.Internal
{
    internal sealed class KafkaClientBrokerLogger : IKafkaBrokerLogger
    {
        [NotNull] private readonly IKafkaClientBroker _broker;
        [NotNull] private readonly IKafkaClientLogger _logger;

        public KafkaClientBrokerLogger([NotNull] IKafkaClientBroker broker, [NotNull] IKafkaClientLogger logger)
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

        public void OnTransportError(KafkaBrokerTransportErrorInfo error)
        {
            try
            {
                _logger.OnBrokerTransportError(_broker, error);
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
