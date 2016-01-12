using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentMember
    {
        public readonly string MemberId;

        public readonly IReadOnlyList<int> PartitionIds;

        public readonly byte[] CustomData;

        public KafkaConsumerAssignmentMember(string memberId, IReadOnlyList<int> partitionIds, byte[] customData)
        {
            MemberId = memberId;
            PartitionIds = partitionIds;
            CustomData = customData;
        }
    }
}
