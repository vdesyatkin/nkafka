using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetCommitResponseTopic
    {
        public string TopicName { get; set; }
        public IReadOnlyList<OffsetCommitResponseTopicPartition> Partitions { get; set; }
    }
}
