using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetCommitRequestTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<OffsetCommitRequestTopicPartition> Partitions { get; set; }
    }
}
