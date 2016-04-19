using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public enum KafkaProdcuerFallbackReason
    {
        Unknown = 0,
        TooLargeSize = 1
    }
}
