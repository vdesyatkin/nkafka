using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    public sealed class KafkaOffsetCommitRequestTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<KafkaOffsetCommitRequestTopicPartition> Partitions;

        public KafkaOffsetCommitRequestTopic(string topicName, IReadOnlyList<KafkaOffsetCommitRequestTopicPartition> partitions)
        {
            TopicName = topicName;
            Partitions = partitions;
        }
    }
}
