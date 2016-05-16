using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    public sealed class KafkaFetchRequestTopic
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        public readonly string TopicName;

        /// <summary>
        /// Partitions.
        /// </summary>
        public readonly IReadOnlyList<KafkaFetchRequestTopicPartition> Partitions;

        public KafkaFetchRequestTopic(string topicName, IReadOnlyList<KafkaFetchRequestTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
