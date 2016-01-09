using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    class OffsetCommitResponse
    {
        public IReadOnlyList<OffsetCommitResponseTopic> Topics { get; set; }
    }
}
