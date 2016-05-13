using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Connection.Diagnostics;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicTransportErrorInfo
    {
        public readonly KafkaBrokerErrorCode BrokerError;

        public readonly string ErrorDescription;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public readonly int BatchSizeByteCount;

        public readonly int BatchSizeMessageCount;

        public KafkaProducerTopicTransportErrorInfo(KafkaBrokerErrorCode brokerError, string errorDescription, [NotNull] IKafkaClientBroker broker, 
            int batchSizeByteCount, int batchSizeMessageCount)
        {
            BrokerError = brokerError;
            ErrorDescription = errorDescription;
            Broker = broker;
            BatchSizeByteCount = batchSizeByteCount;
            BatchSizeMessageCount = batchSizeMessageCount;
        }
    }
}
