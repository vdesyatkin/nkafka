using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetFetchRequestTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<int> PartitionIds { get; set; }
    }
}
