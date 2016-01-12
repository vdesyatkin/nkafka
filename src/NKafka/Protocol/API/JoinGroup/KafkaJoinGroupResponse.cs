using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.JoinGroup
{
    /// <summary>
    /// <para>
    /// The JoinGroup response includes an array for the members of the group along with their metadata. <br/>
    /// This is only populated for the leader to reduce the overall overhead of the protocol; for other members, it will be empty. <br/>
    /// The is used by the leader to prepare member state for phase 2. <br/>
    /// In the case of the consumer, this allows the leader to collect the subscriptions from all members and set the partition assignment. <br/>
    /// The member metadata returned in the join group response corresponds to the respective metadata provided in the join group request for the group protocol chosen by the coordinator.
    /// </para>
    /// </summary>
    [PublicAPI]
    internal sealed class KafkaJoinGroupResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErrorCode;

        public readonly int GroupGenerationId;

        public readonly string GroupProtocolName;

        public readonly string GroupLeaderId;

        public readonly string MemberId;

        public readonly IReadOnlyList<KafkaJoinGroupResponseMember> Members;

        public KafkaJoinGroupResponse(KafkaResponseErrorCode errorCode, int groupGenerationId, string groupProtocolName, string groupLeaderId, string memberId, IReadOnlyList<KafkaJoinGroupResponseMember> members)
        {
            ErrorCode = errorCode;
            GroupGenerationId = groupGenerationId;
            GroupProtocolName = groupProtocolName;
            GroupLeaderId = groupLeaderId;
            MemberId = memberId;
            Members = members;
        }
    }
}
