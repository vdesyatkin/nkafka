using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetCommitRequestTopicPartition
    {
        public int PartitionId { get; set; }

        public long Offset { get; set; }

        public string Metadata { get; set; }        
    }
}
