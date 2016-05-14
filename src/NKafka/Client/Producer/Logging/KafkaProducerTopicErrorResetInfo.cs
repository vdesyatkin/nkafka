using System;
using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicErrorResetInfo
    {
        public readonly int PartitionId;

        public readonly KafkaProducerTopicPartitionErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        public KafkaProducerTopicErrorResetInfo(int partitionId, KafkaProducerTopicPartitionErrorCode errorCode, DateTime? errorTimestampUtc)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
        }
    }
}
