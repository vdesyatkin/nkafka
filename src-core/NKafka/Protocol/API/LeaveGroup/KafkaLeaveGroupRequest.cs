using JetBrains.Annotations;

namespace NKafka.Protocol.API.LeaveGroup
{
    [PublicAPI]
    public sealed class KafkaLeaveGroupRequest : IKafkaRequest
    {
        public readonly string GroupId;

        public readonly string MemberId;

        public KafkaLeaveGroupRequest(string groupId, string memberId)
        {
            GroupId = groupId;
            MemberId = memberId;
        }
    }
}
