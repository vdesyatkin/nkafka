using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class FetchRequestTopic
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string TopicName { get; set; }

        public IReadOnlyList<FetchRequestTopicPartition> Partitions { get; set; }
    }
}
