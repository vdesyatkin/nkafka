using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class FetchResponse
    {
        public IReadOnlyList<FetchResponseTopic> Topics { get; set; }
    }
}
