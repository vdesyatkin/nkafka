using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Connection.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerGroupTransportErrorInfo
    {
        public readonly KafkaBrokerErrorCode BrokerError;

        public readonly string ErrorDescription;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerGroupTransportErrorInfo(KafkaBrokerErrorCode brokerError, string errorDescription, [NotNull] IKafkaClientBroker broker)
        {
            BrokerError = brokerError;
            ErrorDescription = errorDescription;
            Broker = broker;
        }
    }
}
