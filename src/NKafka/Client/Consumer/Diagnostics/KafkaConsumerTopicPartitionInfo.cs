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

        [NotNull] public readonly KafkaConsumerTopicMessagesInfo MessagesInfo;

        [NotNull] public readonly KafkaConsumerTopicPartitionOffsetsInfo OffsetsInfo;

        public KafkaConsumerTopicPartitionInfo(int partitionId, bool isReady, KafkaConsumerTopicPartitionErrorCode? error, DateTime? errorTimestampUtc, 
            [NotNull] KafkaConsumerTopicMessagesInfo messagesInfo, [NotNull] KafkaConsumerTopicPartitionOffsetsInfo offsetsInfo)
        {
            PartitionId = partitionId;
            IsReady = isReady;
            Error = error;
            ErrorTimestampUtc = errorTimestampUtc;
            MessagesInfo = messagesInfo;
            OffsetsInfo = offsetsInfo;
        }
    }
}
