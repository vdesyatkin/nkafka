namespace NKafka.Consumer
{
    public interface IKafkaConsumerSerializer<out TKey, out TData>
    {
        TKey DeserializeKey(byte[] keyBytes);
        TData DeserializeData(byte[] dataBytes);
    }
}
