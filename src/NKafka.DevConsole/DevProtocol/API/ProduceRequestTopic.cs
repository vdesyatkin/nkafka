using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class ProduceRequestTopic
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string TopicName { get; set; }

        public IReadOnlyList<ProduceRequestTopicPartition> Partitions { get; set; }
    }
}
