using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaCodecType : byte
    {
        CodecNone = 0,
        CodecGzip = 1   
    }    
}
