using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    internal sealed class KafkaOffsetFetchResponse : IKafkaResponse
    {
        public readonly IReadOnlyList<KafkaOffsetFetchResponseTopic> Topics;

        public KafkaOffsetFetchResponse(IReadOnlyList<KafkaOffsetFetchResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
