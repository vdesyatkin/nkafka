namespace NKafka.DevConsole.DevProtocol.API
{
    public class LeaveGroupRequest
    {
        public string GroupId { get; set; }
        public string MemberId { get; set; }
    }
}
