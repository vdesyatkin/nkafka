using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetResponseTopicPartition
    {
        public int PartitionId { get; set; }
        public ErrorResponseCode Error { get; set; }
        public IReadOnlyList<long> Offsets { get; set; }
    }
}
