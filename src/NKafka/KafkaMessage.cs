using System;
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

        /// <summary>
        /// The timestamp of the message.
        /// </summary>
        public readonly DateTime TimestampUtc;

        public KafkaMessage(byte[] key, byte[] data, DateTime timestampUtc)
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

        /// <summary>
        /// The timestamp of the message.
        /// </summary>
        public readonly DateTime TimestampUtc;

        public KafkaMessage(TKey key, TData data, DateTime timestampUtc)
        {
            Key = key;
            Data = data;
        }
    }
}