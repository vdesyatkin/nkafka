using System;
using System.Collections.Generic;

namespace NKafka.Client.Producer.Internal
{
    internal interface IKafkaProducerTopicBuffer
    {
        int EnqueuedCount { get; }
        DateTime? EnqueueTimestampUtc { get; }
        IKafkaProducerFallbackHandler FallbackHandler { get; }

        void DistributeMessagesByPartitions(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions);
    }
}
