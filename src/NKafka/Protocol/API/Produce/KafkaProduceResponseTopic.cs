using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponseTopic
    {
        /// <summary>
        /// The topic the offset came from.
        /// </summary>
        public string TopicName { get; private set; }
        
        public IReadOnlyList<KafkaProduceResponseTopicPartition> Partitions { get; private set; }

        public KafkaProduceResponseTopic(string topicName, IReadOnlyList<KafkaProduceResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
