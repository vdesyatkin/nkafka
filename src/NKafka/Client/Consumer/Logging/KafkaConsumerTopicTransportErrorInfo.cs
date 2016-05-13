using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Connection.Diagnostics;

namespace NKafka.Client.Consumer.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerTopicTransportErrorInfo
    {
        public readonly KafkaBrokerErrorCode BrokerError;

        public readonly string ErrorDescription;

        public readonly IKafkaClientBroker Broker;

        public KafkaConsumerTopicTransportErrorInfo(KafkaBrokerErrorCode brokerError, string errorDescription, IKafkaClientBroker broker)
        {
            BrokerError = brokerError;
            ErrorDescription = errorDescription;
            Broker = broker;            
        }
    }
}
