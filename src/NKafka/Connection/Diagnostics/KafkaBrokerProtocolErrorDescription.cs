using JetBrains.Annotations;

namespace NKafka.Connection.Diagnostics
{
    [PublicAPI]
    public enum KafkaBrokerProtocolErrorDescription
    {
        WriteRequest,
        ReadResponseHeader,
        ReadResponse
    }
}
