using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaCodecType : short
    {
        CodecNone = 0,
        CodecGzip = 1   
    }    
}
