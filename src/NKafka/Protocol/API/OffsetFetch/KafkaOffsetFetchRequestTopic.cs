using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    internal sealed class KafkaOffsetFetchRequestTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<int> PartitionIds;

        public KafkaOffsetFetchRequestTopic(string topicName, IReadOnlyList<int> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
        }
    }
}
