using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    // ReSharper disable InconsistentNaming
    public enum KafkaVersion
    {
        V0_8 = 80,
        V0_9 = 90,
        V0_10 = 100,
        V0_10_1 = 110
    }
    // ReSharper restore InconsistentNaming
}