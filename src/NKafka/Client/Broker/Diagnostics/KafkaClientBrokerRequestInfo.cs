using JetBrains.Annotations;
using NKafka.Protocol;

namespace NKafka.Client.Broker.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientBrokerRequestInfo
    {
        public readonly KafkaRequestType RequestType;

        public readonly int RequestId;

        [NotNull] public readonly string Sender;

        public KafkaClientBrokerRequestInfo(KafkaRequestType requestType, int requestId, [NotNull] string sender)
        {
            RequestType = requestType;
            RequestId = requestId;
            Sender = sender;
        }
    }
}
