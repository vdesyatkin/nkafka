using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class ProduceResponseTopic
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string TopicName { get; set; }
        
        public IReadOnlyList<ProduceResponseTopicPartition> Partitions { get; set; }
    }
}
