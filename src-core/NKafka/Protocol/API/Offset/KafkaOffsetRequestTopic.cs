using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Offset
{
    [PublicAPI]
    public sealed class KafkaOffsetRequestTopic
    {
        /// <summary>
        /// The topic name.
        /// </summary>
        public readonly string TopicName;

        /// <summary>
        /// Partitions.
        /// </summary>
        public readonly IReadOnlyList<KafkaOffsetRequestTopicPartition> Partitions;

        public KafkaOffsetRequestTopic(string topicName, IReadOnlyList<KafkaOffsetRequestTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
