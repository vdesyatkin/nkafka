using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetCommitResponseTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<OffsetCommitResponseTopicPartition> Partitions { get; set; }
    }
}
