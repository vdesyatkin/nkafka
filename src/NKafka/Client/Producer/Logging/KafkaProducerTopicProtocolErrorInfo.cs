using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicProtocolErrorInfo
    {
        public readonly int PartitionId;

        public readonly KafkaProducerTopicPartitionErrorCode ProtocolError;

        public readonly string ErrorDescription;

        [NotNull] public readonly IKafkaClientBroker Broker;        

        public readonly int BatchSizeMessageCount;

        public KafkaProducerTopicProtocolErrorInfo(int partitionId, 
            KafkaProducerTopicPartitionErrorCode protocolError, string errorDescription,
            [NotNull] IKafkaClientBroker broker, 
            int batchSizeMessageCount)
        {
            PartitionId = partitionId;
            ProtocolError = protocolError;
            ErrorDescription = errorDescription;
            Broker = broker;            
            BatchSizeMessageCount = batchSizeMessageCount;
        }
    }
}
