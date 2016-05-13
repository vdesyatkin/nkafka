using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerTopicProtocolErrorInfo
    {
        public readonly int PartitionId;

        public readonly KafkaConsumerTopicPartitionErrorCode ProtocolError;

        public readonly string ErrorDescription;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerTopicProtocolErrorInfo(int partitionId,
            KafkaConsumerTopicPartitionErrorCode protocolError, string errorDescription,
            [NotNull] IKafkaClientBroker broker)
        {
            ProtocolError = protocolError;
            ErrorDescription = errorDescription;
            Broker = broker;         
        }
    }
}
