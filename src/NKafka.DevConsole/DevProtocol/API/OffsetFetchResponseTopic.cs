using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetFetchResponseTopic
    {
        public string TopicName { get; set; }

        public IReadOnlyList<OffsetFetchResponseTopicPartition> Partitions { get; set; }
    }
}
