using System;
using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetCommitRequest
    {
        public string GroupId { get; set; }

        public int? GroupGenerationId { get; set; }

        public string MemberId { get; set; }

        public TimeSpan? RetentionTime { get; set; }

        public IReadOnlyList<OffsetCommitRequestTopic> Topics { get; set; }
    }
}
