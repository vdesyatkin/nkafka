using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicPartitionInfo
    {
        public readonly int PartitionId;

        public readonly bool IsAssigned;

        public readonly bool IsReady;

        public readonly bool IsSynchronized;

        public readonly KafkaConsumerTopicPartitionErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtc;

        [NotNull] public readonly KafkaConsumerTopicMessageCountInfo MessageCountInfo;

        [NotNull] public readonly KafkaConsumerTopicMessageSizeInfo MessageSizeInfo;

        [NotNull] public readonly KafkaConsumerTopicPartitionOffsetsInfo OffsetsInfo;

        public KafkaConsumerTopicPartitionInfo(int partitionId, 
            bool isAssigned, bool isReady, bool isSynchronized,
            KafkaConsumerTopicPartitionErrorCode? error, DateTime? errorTimestampUtc, 
            [NotNull] KafkaConsumerTopicMessageCountInfo messageCountInfo,
            [NotNull] KafkaConsumerTopicMessageSizeInfo messageSizeInfo,
            [NotNull] KafkaConsumerTopicPartitionOffsetsInfo offsetsInfo)
        {
            PartitionId = partitionId;
            IsAssigned = isAssigned;
            IsReady = isReady;
            IsSynchronized = isSynchronized;
            Error = error;
            ErrorTimestampUtc = errorTimestampUtc;
            MessageCountInfo = messageCountInfo;
            MessageSizeInfo = messageSizeInfo;
            OffsetsInfo = offsetsInfo;
        }
    }
}
