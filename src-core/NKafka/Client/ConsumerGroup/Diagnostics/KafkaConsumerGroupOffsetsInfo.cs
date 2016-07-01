using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupOffsetsInfo
    {
        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupOffsetsTopicInfo> Topics;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupOffsetsInfo([NotNull] IReadOnlyList<KafkaConsumerGroupOffsetsTopicInfo> topics, DateTime timestampUtc)
        {
            Topics = topics;
            TimestampUtc = timestampUtc;
        }
    }
}
