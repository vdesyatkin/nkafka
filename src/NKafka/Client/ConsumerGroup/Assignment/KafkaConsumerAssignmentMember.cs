using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentMember
    {
        public readonly string MemberId;

        public readonly IReadOnlyList<int> PartitionIds;        

        public KafkaConsumerAssignmentMember(string memberId, IReadOnlyList<int> partitionIds)
        {
            MemberId = memberId;
            PartitionIds = partitionIds;            
        }
    }
}
