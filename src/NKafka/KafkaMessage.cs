namespace NKafka
{
    public sealed class KafkaMessage
    {
        public readonly byte[] Key;
        public readonly byte[] Data;

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