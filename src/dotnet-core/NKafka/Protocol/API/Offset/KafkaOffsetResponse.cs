using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Offset
{
    /// <summary>
    /// The response contains the starting offset of each segment for the requested partition as well as the "log end offset" i.e. the offset of the next message that would be appended to the given partition.
    /// </summary>
    [PublicAPI]
    public sealed class KafkaOffsetResponse : IKafkaResponse
    {
        public readonly IReadOnlyList<KafkaOffsetResponseTopic> Topics;

        public KafkaOffsetResponse(IReadOnlyList<KafkaOffsetResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
