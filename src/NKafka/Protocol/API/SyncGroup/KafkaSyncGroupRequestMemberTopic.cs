using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupRequestMemberTopic
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<int> PartitionIds;

        public KafkaSyncGroupRequestMemberTopic(string topicName, IReadOnlyList<int> partitionIds)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
        }
    }
}
