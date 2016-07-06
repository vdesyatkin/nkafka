using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    public sealed class KafkaOffsetCommitResponseTopicPartition
    {
        public readonly int PartitionId;

        public readonly KafkaResponseErrorCode ErrorCode;

        public KafkaOffsetCommitResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
        }
    }
}
