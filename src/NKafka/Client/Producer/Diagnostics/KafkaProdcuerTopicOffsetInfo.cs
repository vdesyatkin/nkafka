using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProdcuerTopicOffsetInfo
    {
        [NotNull] public readonly string TopicName;

        public DateTime TimestampUtc;

        public long MinOffset;

        public long MaxOffset;

        public KafkaProdcuerTopicOffsetInfo([NotNull] string topicName, DateTime timestampUtc, long minOffset, long maxOffset)
        {
            TopicName = topicName;
            TimestampUtc = timestampUtc;
            MinOffset = minOffset;
            MaxOffset = maxOffset;
        }
    }
}
