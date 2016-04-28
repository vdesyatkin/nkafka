using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionOffsetsInfo
    {
        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupSessionOffsetsTopicInfo> Topics;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupSessionOffsetsInfo([NotNull] IReadOnlyList<KafkaConsumerGroupSessionOffsetsTopicInfo> topics, DateTime timestampUtc)
        {
            Topics = topics;
            TimestampUtc = timestampUtc;
        }
    }
}
