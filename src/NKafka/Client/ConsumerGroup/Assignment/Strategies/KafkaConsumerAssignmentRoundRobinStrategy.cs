using System.Collections.Generic;

namespace NKafka.Client.ConsumerGroup.Assignment.Strategies
{
    public sealed class KafkaConsumerAssignmentRoundRobinStrategy : IKafkaConsumerAssignmentStrategy
    {
        public KafkaConsumerAssignment Assign(KafkaConsumerAssignmentRequest request)
        {                                              
            var assignmentMembers = new List<KafkaConsumerAssignmentMember>(request.Members.Count);

            var memberIndex = 0;
            foreach (var member in request.Members)
            {
                var memberPartitionIds = new List<int>(request.PartitionIds.Count);

                var partitionIndex = 0;
                foreach (var partitionId in request.PartitionIds)
                {
                    if (partitionIndex % request.Members.Count == memberIndex)
                    {
                        memberPartitionIds.Add(partitionId);
                    }

                    partitionIndex++;
                }

                assignmentMembers.Add(new KafkaConsumerAssignmentMember(member.MemberId, memberPartitionIds));
                memberIndex++;
            }

            return new KafkaConsumerAssignment(assignmentMembers);
        }
    }
}
