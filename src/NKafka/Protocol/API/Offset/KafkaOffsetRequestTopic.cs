using System.Collections.Generic;

namespace NKafka.Protocol.API.Offset
{
    internal sealed class KafkaOffsetRequestTopic
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
