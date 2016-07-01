using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentRequest
    {
        [NotNull] public readonly string TopicName;

        [NotNull] public readonly IReadOnlyList<int> PartitionIds;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerAssignmentRequestMember> Members;

        public KafkaConsumerAssignmentRequest([NotNull] string topicName, [NotNull] IReadOnlyList<int> partitionIds, [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerAssignmentRequestMember> members)
        {
            TopicName = topicName;
            PartitionIds = partitionIds;
            Members = members;
        }
    }
}