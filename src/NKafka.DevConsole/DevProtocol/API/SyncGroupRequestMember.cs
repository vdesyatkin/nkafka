using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class SyncGroupRequestMember
    {
        public string MemberId { get; set; }

        public short ProtocolVersion { get; set; }

        public IReadOnlyList<SyncGroupRequestMemberTopic> AssignedTopics { get; set; }

        public byte[] CustomData { get; set; }
    }
}
