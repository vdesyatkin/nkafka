namespace NKafka
{
    public sealed class KafkaMessage
    {
        public byte[] Key { get; private set; }
        public byte[] Data { get; private set; }

        public KafkaMessage(byte[] key, byte[] data)
        {
            Key = key;
            Data = data;
        }
    }

    public sealed class KafkaMessage<TKey, TData>
    {
        public readonly TKey Key;
        public readonly TData Data;

        public KafkaMessage(TKey key, TData data)
        {
            Key = key;
            Data = data;
        }
    }
}