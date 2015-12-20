using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetResponseTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<OffsetResponseTopicPartition> Partitions { get; set; }
    }
}
