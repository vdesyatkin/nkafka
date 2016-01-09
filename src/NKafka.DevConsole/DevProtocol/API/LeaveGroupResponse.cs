using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class LeaveGroupResponse
    {
        public ErrorResponseCode Error { get; set; }
    }
}
