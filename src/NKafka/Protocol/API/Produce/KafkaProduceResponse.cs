using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponse: IKafkaResponse
    {
        public IReadOnlyList<KafkaProduceResponseTopic> Topics { get; private set; }

        public KafkaProduceResponse(IReadOnlyList<KafkaProduceResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
