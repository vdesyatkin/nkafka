namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponseTopicPartition
    {
        /// <summary>
        /// The partition this response entry corresponds to.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// The error from this partition, if any. 
        /// Errors are given on a per-partition basis because a given partition may be unavailable or maintained on a different host, 
        /// while others may have successfully accepted the produce request.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// The offset assigned to the first message in the message set appended to this partition.
        /// </summary>
        public readonly long Offset;

        public KafkaProduceResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode, long offset)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            Offset = offset;
        }
    }
}
