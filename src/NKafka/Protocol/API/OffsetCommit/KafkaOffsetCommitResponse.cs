﻿using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    public sealed class KafkaOffsetCommitResponse : IKafkaResponse
    {
        public readonly IReadOnlyList<KafkaOffsetCommitResponseTopic> Topics;

        public KafkaOffsetCommitResponse(IReadOnlyList<KafkaOffsetCommitResponseTopic> topics)
        {
            Topics = topics;
        }
    }
}
