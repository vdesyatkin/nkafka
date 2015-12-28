using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponseTopic
    {
        /// <summary>
        /// The topic this response entry corresponds to.
        /// </summary>
        public readonly string TopicName;

        /// <summary>
        /// Partitions.
        /// </summary>
        public readonly IReadOnlyList<KafkaProduceResponseTopicPartition> Partitions;

        public KafkaProduceResponseTopic(string topicName, IReadOnlyList<KafkaProduceResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
