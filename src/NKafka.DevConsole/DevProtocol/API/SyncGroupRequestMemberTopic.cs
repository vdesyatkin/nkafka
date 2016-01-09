using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class SyncGroupRequestMemberTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<int> PartitionIds { get; set; }
    }
}
