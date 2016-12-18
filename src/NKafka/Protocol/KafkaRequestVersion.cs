using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    internal enum KafkaRequestVersion : short
    {        
        V0 = 0,
        V1 = 1,
        V2 = 2,
        V3 = 3
    }
}
