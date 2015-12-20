using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetFetchResponseTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<OffsetFetchResponseTopicPartition> Partitions { get; set; }
    }
}
