using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupRequest : IKafkaRequest
    {
        public readonly string GroupName;

        public readonly int GroupGenerationId;

        public readonly string MemberId;

        public readonly IReadOnlyList<KafkaSyncGroupRequestMember> Members;

        public KafkaSyncGroupRequest(string groupName, int groupGenerationId, string memberId, IReadOnlyList<KafkaSyncGroupRequestMember> members)
        {
            GroupName = groupName;
            GroupGenerationId = groupGenerationId;
            MemberId = memberId;
            Members = members;
        }
    }
}
