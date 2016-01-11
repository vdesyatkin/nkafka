using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupRequestMember
    {       
        public string MemberId { get; set; }

        public short ProtocolVersion { get; set; }

        public IReadOnlyList<KafkaSyncGroupRequestMemberTopic> AssignedTopics { get; set; }

        public byte[] CustomData { get; set; }

        public KafkaSyncGroupRequestMember(string memberId, short protocolVersion, IReadOnlyList<KafkaSyncGroupRequestMemberTopic> assignedTopics, byte[] customData)
        {
            MemberId = memberId;
            ProtocolVersion = protocolVersion;
            AssignedTopics = assignedTopics;
            CustomData = customData;
        }
    }
}
