using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetResponseTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<OffsetResponseTopicPartition> Partitions { get; set; }
    }
}
