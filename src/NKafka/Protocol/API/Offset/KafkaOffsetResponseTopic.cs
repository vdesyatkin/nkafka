using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Offset
{
    [PublicAPI]
    public sealed class KafkaOffsetResponseTopic
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
