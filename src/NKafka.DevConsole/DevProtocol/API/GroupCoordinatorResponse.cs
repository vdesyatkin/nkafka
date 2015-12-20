namespace NKafka.DevConsole.DevProtocol.API
{
    public class GroupCoordinatorResponse
    {
        public ErrorResponseCode Error { get; set; }

        public int CoordinatorId { get; set; }

        public string CoordinatorHost { get; set; }

        public int CoordinatorPort { get; set; }
    }
}
