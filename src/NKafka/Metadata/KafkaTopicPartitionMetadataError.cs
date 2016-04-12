using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public enum KafkaTopicPartitionMetadataError : byte
    {
        UnknownError = 0,

        /// <summary>
        /// This request is for a partition that does not exist on this broker.
        /// </summary>
        UnknownPartition = 3,

        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5
    }
}
