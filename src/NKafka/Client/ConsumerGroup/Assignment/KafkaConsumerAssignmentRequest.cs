using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentRequest
    {
        public readonly string TopicName;

        public readonly IReadOnlyList<int> PartitionIds;

        public readonly IReadOnlyList<KafkaConsumerAssignmentRequestMember> Members;

        public KafkaConsumerAssignmentRequest(string topicName, IReadOnlyList<int> partitionIds, IReadOnlyList<KafkaConsumerAssignmentRequestMember> members)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
            Members = members;
        }
    }
}