using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class TopicMetadataRequest
    {
        public IReadOnlyList<string> TopicNames { get; set; }
    }
}
