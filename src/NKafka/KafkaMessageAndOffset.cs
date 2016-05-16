using System;
using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public sealed class KafkaMessageAndOffset
    {
        public readonly long Offset;

        public readonly byte[] Key;

        public readonly byte[] Data;

        public readonly DateTime? TiemestampUtc;

        public KafkaMessageAndOffset(long offset, byte[] key, byte[] data, DateTime? timestampUtc)
        {
            Offset = offset;
            Key = key;
            Data = data;
        }
    }    
}
