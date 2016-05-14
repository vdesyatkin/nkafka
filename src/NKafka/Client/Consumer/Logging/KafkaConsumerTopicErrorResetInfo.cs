using System;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Logging
{    
    [PublicAPI]
    public sealed class KafkaConsumerTopicErrorResetInfo
    {
        public readonly int PartitionId;

        public readonly KafkaConsumerTopicPartitionErrorCode ErrorCode;

        public readonly DateTime? ErrorTimestampUtc;

        public KafkaConsumerTopicErrorResetInfo(int partitionId, KafkaConsumerTopicPartitionErrorCode errorCode, DateTime? errorTimestampUtc)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            ErrorTimestampUtc = errorTimestampUtc;
        }
    }
}
