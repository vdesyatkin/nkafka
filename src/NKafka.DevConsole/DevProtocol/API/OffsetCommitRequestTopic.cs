using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetCommitRequestTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<OffsetCommitRequestTopicPartition> Partitions { get; set; }
    }
}
