using System.Collections.Generic;

namespace NKafka.Client.Producer
{
    public interface IKafkaProducerPartitioner
    {
        int GetPartition(byte[] key, byte[] data, IReadOnlyList<int> partitions);
    }

    public interface IKafkaProducerPartitioner<TKey, TData>
    {
        int GetPartition(TKey key, TData data, IReadOnlyList<int> partitions);
    }
}
