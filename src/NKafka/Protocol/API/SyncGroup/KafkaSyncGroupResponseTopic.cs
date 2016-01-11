using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupResponseTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<int> PartitionIds;

        public KafkaSyncGroupResponseTopic(string topicName, IReadOnlyList<int> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
        }
    }
}
