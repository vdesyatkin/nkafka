using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetResponseTopicPartition
    {
        public int PartitionId { get; set; }
        public ErrorResponseCode Error { get; set; }
        public IReadOnlyList<long> Offsets { get; set; }
    }
}
