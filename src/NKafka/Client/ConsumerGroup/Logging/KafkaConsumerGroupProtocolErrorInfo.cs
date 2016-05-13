using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerGroupProtocolErrorInfo
    {        
        public readonly KafkaConsumerGroupErrorCode ProtocolError;

        public readonly string ErrorDescription;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerGroupProtocolErrorInfo(
            KafkaConsumerGroupErrorCode protocolError, 
            string errorDescription,
            [NotNull] IKafkaClientBroker broker)
        {
            ProtocolError = protocolError;
            ErrorDescription = errorDescription;
            Broker = broker;
        }
    }
}
