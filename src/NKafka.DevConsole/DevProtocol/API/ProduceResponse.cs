using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class ProduceResponse
    {
        public IReadOnlyList<ProduceResponseTopic> Topics { get; set; }
    }
}
