using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    public sealed class KafkaFetchResponseTopic
    {
        /// <summary>
        /// The name of the topic this response entry is for.
        /// </summary>
        public readonly string TopicName;

        /// <summary>
        /// Partitions.
        /// </summary>
        public readonly IReadOnlyList<KafkaFetchResponseTopicPartition> Partitions;

        public KafkaFetchResponseTopic(string topicName, IReadOnlyList<KafkaFetchResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
