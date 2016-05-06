using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public enum KafkaProdcuerFallbackErrorCode
    {
        UnknownError = 0,
        TooLargeSize = 1,
        ClientStopping = 2
    }
}
