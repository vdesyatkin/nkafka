using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverOffsetsInfo
    {
        [NotNull]
        public readonly IReadOnlyList<KafkaConsumerGroupObserverOffsetsTopicInfo> Topics;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupObserverOffsetsInfo([NotNull] IReadOnlyList<KafkaConsumerGroupObserverOffsetsTopicInfo> topics, DateTime timestampUtc)
        {
            Topics = topics;
            TimestampUtc = timestampUtc;
        }
    }
}
