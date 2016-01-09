using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class GroupCoordinatorResponse
    {
        public ErrorResponseCode Error { get; set; }

        public int CoordinatorId { get; set; }

        public string CoordinatorHost { get; set; }

        public int CoordinatorPort { get; set; }
    }
}
