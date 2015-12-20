namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponseTopicPartition
    {
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; private set; }
        /// <summary>
        /// ErrorCode response code.  0 is success.
        /// </summary>
        public KafkaResponseErrorCode ErrorCode { get; private set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; private set; }

        public KafkaProduceResponseTopicPartition(int partitionId, KafkaResponseErrorCode errorCode, long offset)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            Offset = offset;
        }
    }
}
