using JetBrains.Annotations;
using NKafka.Protocol;

namespace NKafka.Connection.Logging
{
    [PublicAPI]
    public sealed class KafkaBrokerRequestInfo
    {
        public readonly KafkaRequestType RequestType;

        public readonly int RequestId;

        [NotNull] public readonly string Sender;

        public KafkaBrokerRequestInfo(KafkaRequestType requestType, int requestId, [NotNull] string sender)
        {
            RequestType = requestType;
            RequestId = requestId;
            Sender = sender;
        }
    }
}
