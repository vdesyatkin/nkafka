using System;
using System.Collections.Generic;

namespace NKafka.Client.Producer.Internal
{
    internal interface IKafkaProducerTopicBuffer
    {
        long EnqueuedCount { get; }
        DateTime? EnqueueTimestampUtc { get; }
        void Flush(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions);
    }
}
