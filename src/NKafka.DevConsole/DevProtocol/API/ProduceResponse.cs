using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class ProduceResponse
    {
        public IReadOnlyList<ProduceResponseTopic> Topics { get; set; }
    }
}
