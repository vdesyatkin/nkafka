using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetFetchResponse
    {
        public IReadOnlyList<OffsetFetchResponseTopic> Topics { get; set; }
    }
}
