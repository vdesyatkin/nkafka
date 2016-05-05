using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionInfo
    {
        public readonly int PartitionId;

        public readonly bool IsReady;

        public readonly KafkaProducerTopicPartitionErrorCode? Error;

        public readonly DateTime? ErrorTimestampUtc;

        [NotNull] public readonly KafkaProducerTopicMessageCountInfo MessageCountInfo;

        [NotNull] public readonly KafkaProducerTopicMessageSizeInfo MessageSizeInfo;

        [NotNull] public readonly KafkaProducerTopicPartitionLimitInfo LimitInfo;

        public KafkaProducerTopicPartitionInfo(int partitionId, bool isReady, 
            KafkaProducerTopicPartitionErrorCode? error, DateTime? errorTimestampUtc,
            [NotNull] KafkaProducerTopicMessageCountInfo messageCountInfo,
            [NotNull] KafkaProducerTopicMessageSizeInfo messageSizeInfo,
            [NotNull] KafkaProducerTopicPartitionLimitInfo limitInfo)
        {
            PartitionId = partitionId;
            IsReady = isReady;
            Error = error;
            MessageCountInfo = messageCountInfo;
            MessageSizeInfo = messageSizeInfo;
            LimitInfo = limitInfo;
        }
    }
}
