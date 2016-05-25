using System;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerTopicErrorResetInfo
    {
        public readonly int PartitionId;

        public readonly KafkaConsumerTopicPartitionErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        [NotNull] public readonly IKafkaClientBroker Broker;

        public KafkaConsumerTopicErrorResetInfo(int partitionId, KafkaConsumerTopicPartitionErrorCode errorCode, DateTime? errorTimestampUtc,
            [NotNull] IKafkaClientBroker broker)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
            Broker = broker;
        }
    }
}
