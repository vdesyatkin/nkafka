using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class SyncGroupRequestMemberTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<int> PartitionIds { get; set; }
    }
}
