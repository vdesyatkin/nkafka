using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public interface IKafkaConsumerSerializer<out TKey, out TData>
    {
        TKey DeserializeKey(byte[] keyBytes);
        TData DeserializeData(byte[] dataBytes);
    }
}
