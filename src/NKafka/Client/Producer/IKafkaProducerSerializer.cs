using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public interface IKafkaProducerSerializer<TKey, TData>
    {
        byte[] SerializeKey(TKey key);
        byte[] SerializeData(TData data);
        TKey DeserializeKey(byte[] keyBytes);
        TData DeserializeData(byte[] dataBytes);
    }
}
