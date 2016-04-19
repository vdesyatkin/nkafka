using JetBrains.Annotations;

namespace NKafka
{
    [PublicAPI]
    public enum KafkaTimestampType : short
    {
        CreateTime = 0,
        LogAppendTime = 1
    }
}
