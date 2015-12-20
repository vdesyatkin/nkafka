namespace NKafka.Producer
{
    public interface IKafkaProducerSerializer<TKey, TData>
    {
        byte[] SerializeKey(TKey key);
        byte[] SerializeValue(TData data);
    }
}
