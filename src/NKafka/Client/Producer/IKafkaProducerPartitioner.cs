using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerPartitioner
    {
        int GetPartition(byte[] key, byte[] data, IReadOnlyList<int> partitions);
    }

    [PublicAPI]
    public interface IKafkaProducerPartitioner<in TKey, in TData>
    {
        int GetPartition(TKey key, TData data, IReadOnlyList<int> partitions);
    }
}
