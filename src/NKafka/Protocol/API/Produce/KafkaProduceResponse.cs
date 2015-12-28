using System.Collections.Generic;

namespace NKafka.Protocol.API.Produce
{
    internal sealed class KafkaProduceResponse: IKafkaResponse
    {
        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaProduceResponseTopic> Topics;

        public KafkaProduceResponse(IReadOnlyList<KafkaProduceResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
