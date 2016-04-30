using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicInfo
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly IReadOnlyList<KafkaConsumerTopicPartitionInfo> Partitions;
        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicInfo([NotNull] string topicName, [NotNull] IReadOnlyList<KafkaConsumerTopicPartitionInfo> partitions, DateTime timestampUtc)
        {
            TopicName = topicName;
            Partitions = partitions;
            TimestampUtc = timestampUtc;
        }
    }
}
