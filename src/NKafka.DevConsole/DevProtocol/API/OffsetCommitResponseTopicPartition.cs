using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetCommitResponseTopicPartition
    {
        public int PartitionId { get; set; }
        public ErrorResponseCode Error { get; set; }
    }
}
