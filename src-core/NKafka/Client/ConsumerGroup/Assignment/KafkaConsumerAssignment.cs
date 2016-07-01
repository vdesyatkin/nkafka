using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignment
    {
        public readonly IReadOnlyList<KafkaConsumerAssignmentMember> Members;

        public KafkaConsumerAssignment(IReadOnlyList<KafkaConsumerAssignmentMember> members)
        {
            Members = members;
        }
    }
}
