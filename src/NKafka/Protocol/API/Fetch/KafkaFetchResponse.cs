using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    [PublicAPI]
    internal sealed class KafkaFetchResponse : IKafkaResponse
    {
        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaFetchResponseTopic> Topics;

        public KafkaFetchResponse(IReadOnlyList<KafkaFetchResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
