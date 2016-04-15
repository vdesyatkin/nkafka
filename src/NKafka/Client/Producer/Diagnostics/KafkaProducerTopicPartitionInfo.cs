using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionInfo
    {
        public readonly int PartitionId;

        public readonly bool IsProducing;

        public readonly KafkaProducerTopicPartitionErrorCode? Error;

        [NotNull]
        public readonly KafkaProducerTopicMessageCountInfo MessagesInfo;

        [CanBeNull]
        public readonly KafkaProducerTopicPartitionOffsetInfo OffsetInfo;

        public KafkaProducerTopicPartitionInfo(int partitionId, bool isProducing, KafkaProducerTopicPartitionErrorCode? error, 
            [NotNull] KafkaProducerTopicMessageCountInfo messagesInfo,
            [CanBeNull] KafkaProducerTopicPartitionOffsetInfo offsetInfo)
        {
            PartitionId = partitionId;
            IsProducing = isProducing;
            Error = error;
            MessagesInfo = messagesInfo;
            OffsetInfo = offsetInfo;
        }
    }
}
