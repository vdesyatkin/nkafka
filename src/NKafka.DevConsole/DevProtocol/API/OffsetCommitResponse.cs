using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    class OffsetCommitResponse
    {
        public IReadOnlyList<OffsetCommitResponseTopic> Topics { get; set; }
    }
}
