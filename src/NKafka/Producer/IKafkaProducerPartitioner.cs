namespace NKafka.Producer
{
    public interface IKafkaProducerPartitioner
    {
        int GetPartitionIndex(byte[] key, byte[] data, int partitionCount);
    }

    public interface IKafkaProducerPartitioner<TKey, TData>
    {
        int GetPartitionIndex(TKey key, TData data, int partitionCount);
    }
}
