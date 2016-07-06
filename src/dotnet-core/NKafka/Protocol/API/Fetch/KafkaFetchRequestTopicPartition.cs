using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    public sealed class KafkaFetchRequestTopicPartition
    {
        /// <summary>
        /// The id of the partition the fetch is for.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// The offset to begin this fetch from.
        /// </summary>
        public readonly long FetchOffset;

        /// <summary>
        /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// </summary>
        public readonly int MaxBytes;

        public KafkaFetchRequestTopicPartition(int partitionId, long fetchOffset, int maxBytes)
        {
            PartitionId = partitionId;
            FetchOffset = fetchOffset;
            MaxBytes = maxBytes;
        }
    }
}
