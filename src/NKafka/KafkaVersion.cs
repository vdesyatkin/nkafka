using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    // ReSharper disable InconsistentNaming
    public enum KafkaVersion : short
    {        
        V0_8 = 8,
        V0_9 = 9,
        V0_10 = 10    
    }
    // ReSharper restore InconsistentNaming
}
