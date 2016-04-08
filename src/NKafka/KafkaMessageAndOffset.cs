using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
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
}
