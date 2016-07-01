using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
    public sealed class KafkaProduceRequestTopic
    {
        /// <summary>
        /// The topic that data is being published to.
        /// </summary>
        public readonly string TopicName;

        public readonly IReadOnlyList<KafkaProduceRequestTopicPartition> Partitions;

        public KafkaProduceRequestTopic(string topicName, IReadOnlyList<KafkaProduceRequestTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
