using System.Collections.Generic;

namespace NKafka.Protocol.API.Offset
{
    internal sealed class KafkaOffsetResponseTopicPartition
    {
        public readonly int PartitionId;

        public readonly KafkaResponseErrorCode ErrorCode;

        public readonly IReadOnlyList<long> Offsets;

        public KafkaOffsetResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode,
            IReadOnlyList<long> offsets)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            Offsets = offsets;
        }
    }
}
