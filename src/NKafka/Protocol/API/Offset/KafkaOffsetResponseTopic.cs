using System;
using System.Collections.Generic;

namespace NKafka.Protocol.API.Offset
{
    internal sealed class KafkaOffsetResponseTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<KafkaOffsetResponseTopicPartition> Partitions;

        public KafkaOffsetResponseTopic(string topicName, IReadOnlyList<KafkaOffsetResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
