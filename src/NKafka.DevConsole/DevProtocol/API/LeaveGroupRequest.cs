using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class LeaveGroupRequest
    {
        public string GroupId { get; set; }
        public string MemberId { get; set; }
    }
}
