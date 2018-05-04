using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    public sealed class KafkaOffsetFetchResponse : IKafkaResponse
    {
        public readonly IReadOnlyList<KafkaOffsetFetchResponseTopic> Topics;

        public readonly KafkaResponseErrorCode ErrorCode;

        public KafkaOffsetFetchResponse(IReadOnlyList<KafkaOffsetFetchResponseTopic> topics, KafkaResponseErrorCode errorCode)
        {
            Topics = topics;
            ErrorCode = errorCode;
        }
    }
}