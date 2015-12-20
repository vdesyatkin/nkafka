using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class TopicMetadataResponse
    {
        public IReadOnlyList<TopicMetadataResponseBroker> Brokers { get; set; }
        public IReadOnlyList<TopicMetadataResponseTopic> Topics { get; set; }
    }
}
