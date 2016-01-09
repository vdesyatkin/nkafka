using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class HeartbeatResponse
    {
        public ErrorResponseCode Erorr { get; set; }
    }
}
