using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaTimestampType : byte
    {
        CreateTime = 0,
        LogAppendTime = 1
    }
}
