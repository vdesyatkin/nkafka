namespace NKafka.DevConsole.DevProtocol.API
{
    public class SyncGroupResponse
    {
        public ErrorResponseCode Error { get; set; }

        public SyncGroupResponseAssignment Assignment { get; set; }
    }
}
