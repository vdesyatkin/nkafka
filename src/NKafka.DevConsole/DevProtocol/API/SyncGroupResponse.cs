using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class SyncGroupResponse
    {
        public ErrorResponseCode Error { get; set; }

        public SyncGroupResponseAssignment Assignment { get; set; }
    }
}
