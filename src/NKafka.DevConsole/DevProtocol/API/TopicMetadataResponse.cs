using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class TopicMetadataResponse
    {
        public IReadOnlyList<TopicMetadataResponseBroker> Brokers { get; set; }
        public IReadOnlyList<TopicMetadataResponseTopic> Topics { get; set; }
    }
}
