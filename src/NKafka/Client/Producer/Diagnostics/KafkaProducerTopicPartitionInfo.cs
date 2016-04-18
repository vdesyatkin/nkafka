using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitionInfo
    {
        public readonly int PartitionId;

        public readonly bool IsReady;

        public readonly KafkaProducerTopicPartitionErrorCode? Error;

        [NotNull]
        public readonly KafkaProducerTopicMessageCountInfo MessagesInfo;        

        public KafkaProducerTopicPartitionInfo(int partitionId, bool isReady, KafkaProducerTopicPartitionErrorCode? error, 
            [NotNull] KafkaProducerTopicMessageCountInfo messagesInfo)
        {
            PartitionId = partitionId;
            IsReady = isReady;
            Error = error;
            MessagesInfo = messagesInfo;            
        }
    }
}
