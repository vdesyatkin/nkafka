namespace NKafka.DevConsole.DevProtocol.API
{
    public class HeartbeatRequest
    {
        public string GroupId { get; set; }

        public int? GroupGenerationId { get; set; }

        public string MemberId { get; set; }
    }
}
