using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetFetch
{
    [PublicAPI]
    internal sealed class KafkaOffsetFetchRequest : IKafkaRequest
    {
        public readonly string GroupName;

        public readonly IReadOnlyList<KafkaOffsetFetchRequestTopic> Topics;

        public KafkaOffsetFetchRequest(string groupName, IReadOnlyList<KafkaOffsetFetchRequestTopic> topics)
        {
            GroupName = groupName;
            Topics = topics;
        }
    }
}
