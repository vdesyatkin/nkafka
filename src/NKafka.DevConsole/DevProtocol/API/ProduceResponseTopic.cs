using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class ProduceResponseTopic
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string TopicName { get; set; }
        
        public IReadOnlyList<ProduceResponseTopicPartition> Partitions { get; set; }
    }
}
