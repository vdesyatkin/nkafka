using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class SyncGroupRequest
    {
        public string GroupId { get; set; }
        public int GroupGenerationId { get; set; }
        public string MemberId { get; set; }
        public IReadOnlyList<SyncGroupRequestMember> Members { get; set; }
    }
}
