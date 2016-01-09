using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class ProduceResponseTopicPartition
    {
        /// <summary>
        /// The partition the offset came from.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// Error response code.  0 is success.
        /// </summary>
        public ErrorResponseCode Error { get; set; }
        /// <summary>
        /// The offset number to commit as completed.
        /// </summary>
        public long Offset { get; set; }
    }
}
