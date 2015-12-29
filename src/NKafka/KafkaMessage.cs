using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public sealed class KafkaMessage
    {
        /// <summary>
        /// The key is an optional message key that was used for partition assignment. The key can be null.
        /// </summary>
        public readonly byte[] Key;

        /// <summary>
        /// The data is the actual message contents as an opaque byte array.
        /// </summary>
        public readonly byte[] Data;

        public KafkaMessage(byte[] key, byte[] data)
        {
            Key = key;
            Data = data;
        }
    }

    [PublicAPI]
    public sealed class KafkaMessage<TKey, TData>
    {
        /// <summary>
        /// The key is an optional message key that was used for partition assignment. The key can be null.
        /// </summary>
        public readonly TKey Key;

        /// <summary>
        /// The data is the actual message contents as an opaque byte array.
        /// </summary>
        public readonly TData Data;

        public KafkaMessage(TKey key, TData data)
        {
            Key = key;
            Data = data;
        }
    }
}