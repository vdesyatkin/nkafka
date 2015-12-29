using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Produce
{
    [PublicAPI]
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
