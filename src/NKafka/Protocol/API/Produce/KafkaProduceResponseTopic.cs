using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
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
