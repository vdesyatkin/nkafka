using System;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicErrorResetInfo
    {
        public readonly int PartitionId;

        public readonly KafkaProducerTopicPartitionErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaProducerTopicErrorResetInfo(int partitionId, KafkaProducerTopicPartitionErrorCode errorCode, DateTime? errorTimestampUtc,
            [NotNull] IKafkaClientBroker broker)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
            Broker = broker;
        }
    }
}
