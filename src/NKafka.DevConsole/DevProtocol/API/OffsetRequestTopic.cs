using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetRequestTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<OffsetRequestTopicPartition> Partitions { get; set; }
    }
}
