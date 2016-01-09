using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class HeartbeatRequest
    {
        public string GroupId { get; set; }

        public int? GroupGenerationId { get; set; }

        public string MemberId { get; set; }
    }
}
