using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class SyncGroupRequest
    {
        public string GroupId { get; set; }
        public int GroupGenerationId { get; set; }
        public string MemberId { get; set; }
        public IReadOnlyList<SyncGroupRequestMember> Members { get; set; }
    }
}
