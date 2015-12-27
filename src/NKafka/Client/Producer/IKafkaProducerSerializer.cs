namespace NKafka.Client.Producer
{
    public interface IKafkaProducerSerializer<in TKey, in TData>
    {
        byte[] SerializeKey(TKey key);
        byte[] SerializeData(TData data);
    }
}
