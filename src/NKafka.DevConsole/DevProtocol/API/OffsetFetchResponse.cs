using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetFetchResponse
    {
        public IReadOnlyList<OffsetFetchResponseTopic> Topics { get; set; }
    }
}
