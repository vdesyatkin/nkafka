using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class JoinGroupResponse
    {
        public ErrorResponseCode Erorr { get; set; }

        public int GroupGenerationId { get; set; }

        public string GroupProtocol { get; set; }

        public string GroupLeaderId { get; set; }

        public string MemberId { get; set; }

        public IReadOnlyList<JoinGroupResponseMember> Members { get; set; }
    }
}
