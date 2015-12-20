using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class FetchResponseTopic
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public string TopicName { get; set; }

        public IReadOnlyList<FetchResponseTopicPartition> Partitions { get; set; }
    }
}
