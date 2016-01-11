using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.OffsetCommit
{
    [PublicAPI]
    internal sealed class KafkaOffsetCommitRequest : IKafkaRequest
    {
        public readonly string GroupName;

        public readonly int GroupGenerationId;

        public readonly string MemberId;

        public readonly TimeSpan RetentionTime;

        public readonly IReadOnlyList<KafkaOffsetCommitRequestTopic> Topics;

        public KafkaOffsetCommitRequest(string groupName, int groupGenerationId, string memberId, TimeSpan retentionTime, IReadOnlyList<KafkaOffsetCommitRequestTopic> topics)
        {
            GroupName = groupName;
            GroupGenerationId = groupGenerationId;
            MemberId = memberId;
            RetentionTime = retentionTime;
            Topics = topics;
        }
    }
}
