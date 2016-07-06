using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    public sealed class KafkaOffsetFetchResponseTopicPartition
    {
        public readonly int PartitionId;

        public readonly KafkaResponseErrorCode ErrorCode;

        public readonly long? Offset;

        public string Metadata;

        public KafkaOffsetFetchResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode, long? offset, string metadata)
        {
            ErrorCode = errorCode;
            PartitionId = partitionId;
            Offset = offset;
            Metadata = metadata;
        }
    }
}
