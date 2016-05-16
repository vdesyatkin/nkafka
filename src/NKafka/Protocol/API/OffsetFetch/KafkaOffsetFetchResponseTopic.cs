using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    public sealed class KafkaOffsetFetchResponseTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<KafkaOffsetFetchResponseTopicPartition> Partitions;

        public KafkaOffsetFetchResponseTopic(string topicName, IReadOnlyList<KafkaOffsetFetchResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
