using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    internal sealed class KafkaOffsetCommitResponseTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<KafkaOffsetCommitResponseTopicPartition> Partitions;

        public KafkaOffsetCommitResponseTopic(string topicName, IReadOnlyList<KafkaOffsetCommitResponseTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
