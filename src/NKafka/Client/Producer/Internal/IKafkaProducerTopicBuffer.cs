using System.Collections.Generic;

namespace NKafka.Client.Producer.Internal
{
    internal interface IKafkaProducerTopicBuffer
    {
        void Flush(IReadOnlyList<int> partitionIds, IReadOnlyDictionary<int, KafkaProducerTopicPartition> partitions);
    }
}
