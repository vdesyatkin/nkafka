using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class FetchResponseTopic
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public string TopicName { get; set; }

        public IReadOnlyList<FetchResponseTopicPartition> Partitions { get; set; }
    }
}
