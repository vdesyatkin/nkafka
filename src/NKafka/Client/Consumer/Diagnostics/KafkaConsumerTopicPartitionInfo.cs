using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicPartitionInfo
    {
        public readonly int PartitionId;

        public readonly bool IsReady;

        public readonly KafkaConsumerTopicPartitionErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtc;

        public KafkaConsumerTopicPartitionInfo(int partitionId, bool isReady, KafkaConsumerTopicPartitionErrorCode? error, DateTime? errorTimestampUtc)
        {
            PartitionId = partitionId;
            IsReady = isReady;
            Error = error;
            ErrorTimestampUtc = errorTimestampUtc;
        }
    }
}
