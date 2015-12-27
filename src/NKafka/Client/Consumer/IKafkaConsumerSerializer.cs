namespace NKafka.Client.Consumer
{
    public interface IKafkaConsumerSerializer<out TKey, out TData>
    {
        TKey DeserializeKey(byte[] keyBytes);
        TData DeserializeData(byte[] dataBytes);
    }
}
