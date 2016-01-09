using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class ProduceRequestTopic
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string TopicName { get; set; }

        public IReadOnlyList<ProduceRequestTopicPartition> Partitions { get; set; }
    }
}
