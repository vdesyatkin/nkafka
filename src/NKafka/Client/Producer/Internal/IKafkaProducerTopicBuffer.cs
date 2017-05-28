using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal interface IKafkaProducerTopicBuffer
    {
        [NotNull] string TopicName { get; }

        int EnqueuedCount { get; }
        DateTime? EnqueueTimestampUtc { get; }

        [CanBeNull] IKafkaProducerFallbackHandler FallbackHandler { get; }

        int DistributeMessagesByPartitions(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions);
    }
}