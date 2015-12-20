using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetFetchRequest
    {
        public string GroupId { get; set; }

        public IReadOnlyList<OffsetFetchRequestTopic> Topics { get; set; }
    }
}
