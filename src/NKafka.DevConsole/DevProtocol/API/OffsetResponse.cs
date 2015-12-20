using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetResponse
    {
        public IReadOnlyList<OffsetResponseTopic> Topics { get; set; }
    }
}
