using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment.Strategies
{
    public sealed class KafkaConsumerAssignmentRoundRobinStrategy : IKafkaConsumerAssignmentStrategy
    {
        [NotNull]
        private static readonly KafkaMemberComparer MemberComparer = new KafkaMemberComparer();

        public KafkaConsumerAssignment Assign(KafkaConsumerAssignmentRequest request)
        {
            if (request == null)
            {
                return new KafkaConsumerAssignment(new KafkaConsumerAssignmentMember[0]);
            }

            var sortedMembers = new List<KafkaConsumerAssignmentRequestMember>(request.Members);
            sortedMembers.Sort(MemberComparer);

            var assignmentMembers = new List<KafkaConsumerAssignmentMember>(sortedMembers.Count);

            var memberIndex = 0;
            foreach (var member in sortedMembers)
            {
                var memberPartitionIds = new List<int>(request.PartitionIds.Count);

                var partitionIndex = 0;
                foreach (var partitionId in request.PartitionIds)
                {
                    if (partitionIndex % sortedMembers.Count == memberIndex)
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

        private sealed class KafkaMemberComparer : IComparer<KafkaConsumerAssignmentRequestMember>
        {
            public int Compare(KafkaConsumerAssignmentRequestMember x, KafkaConsumerAssignmentRequestMember y)
            {
                var member1 = x?.MemberId;
                var member2 = y?.MemberId;

                if (member1 == null && member2 == null)
                {
                    return 0;
                }

                if (member1 == null)
                {
                    return -1;
                }

                if (member2 == null)
                {
                    return 1;
                }

                return string.Compare(member1, member2, StringComparison.Ordinal);
            }
        }
    }
}