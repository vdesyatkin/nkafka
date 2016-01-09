using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class FetchResponseTopicPartition
    {
        /// <summary>
        /// The id of the partition this response is for.
        /// </summary>
        public int PartitionId { get; set; }

        public ErrorResponseCode Error { get; set; }

        /// <summary>
        /// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
        /// </summary>
        public long HighwaterMarkOffset { get; set; }

        public MessageSet MessageSet { get; set; }
    }
}
