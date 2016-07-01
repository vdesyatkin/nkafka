using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Assignment
{
    [PublicAPI]
    public sealed class KafkaConsumerAssignmentRequestMember
    {
        public readonly string MemberId;

        public readonly bool IsLeader;        

        public readonly byte[] CustomData;

        public KafkaConsumerAssignmentRequestMember(string memberId, bool isLeader, byte[] customData)
        {
            MemberId = memberId;
            IsLeader = isLeader;            
            CustomData = customData;
        }
    }
}
