using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public class KafkaKeyHashCodePartitioner : IKafkaProducerPartitioner
    {
        public int GetPartition(KafkaMessage message, IReadOnlyList<int> partitions)
        {
            if (message.Key == null) return 0;
            return message.Key.GetHashCode() % partitions.Count;
        }
    }

    [PublicAPI]
    public class KafkaKeyHashCodePartitioner<TKey, TData> : IKafkaProducerPartitioner<TKey, TData>
    {
        public int GetPartition(KafkaMessage<TKey, TData> message, IReadOnlyList<int> partitions)
        {
            if (message.Key == null) return 0;
            return message.Key.GetHashCode() % partitions.Count;
        }
    }
}
