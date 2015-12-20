using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceRequestTopic
    {
        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string TopicName { get; private set; }

        public IReadOnlyList<KafkaProduceRequestTopicPartition> Partitions { get; private set; }

        public KafkaProduceRequestTopic(string topicName, IReadOnlyList<KafkaProduceRequestTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
