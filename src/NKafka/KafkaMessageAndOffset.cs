namespace NKafka
{
    internal sealed class KafkaMessageAndOffset
    {
        public readonly long Offset;
        public readonly byte[] Key;
        public readonly byte[] Data;

        public KafkaMessageAndOffset(long offset, byte[] key, byte[] data)
        {
            Offset = offset;
            Key = key;
            Data = data;
        }
    }

    internal sealed class KafkaMessageAndOffset<TKey, TData>
    {
        public readonly long Offset;
        public readonly TKey Key;
        public readonly TData Data;

        public KafkaMessageAndOffset(long offset, TKey key, TData data)
        {
            Offset = offset;
            Key = key;
            Data = data;
        }
    }
}
