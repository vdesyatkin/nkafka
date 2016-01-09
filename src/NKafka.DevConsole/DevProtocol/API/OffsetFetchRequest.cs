using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class OffsetFetchRequest
    {
        public string GroupId { get; set; }

        public IReadOnlyList<OffsetFetchRequestTopic> Topics { get; set; }
    }
}
