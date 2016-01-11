using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    internal sealed class KafkaOffsetCommitRequestTopicPartition
    {
        public readonly int PartitionId;

        public readonly long Offset;

        public readonly string Metadata;

        public KafkaOffsetCommitRequestTopicPartition(int partitionId, long offset, string metadata)
        {
            PartitionId = partitionId;
            Offset = offset;
            Metadata = metadata;
        }        
    }
}
