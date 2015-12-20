namespace NKafka.DevConsole.DevProtocol.API
{
    public class FetchRequestTopicPartition
    {
        /// <summary>
        /// The id of the partition the fetch is for.
        /// </summary>
        public int PartitionId { get; set; }

        /// <summary>
        /// The offset to begin this fetch from.
        /// </summary>
        public long FetchOffset { get; set; }

        /// <summary>
        /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// </summary>
        public int MaxBytes { get; set; }
    }
}