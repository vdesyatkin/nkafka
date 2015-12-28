using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceRequestTopic
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
