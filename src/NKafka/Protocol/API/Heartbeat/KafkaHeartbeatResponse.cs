using JetBrains.Annotations;

namespace NKafka.Protocol.API.Heartbeat
{
    [PublicAPI]
    internal sealed class KafkaHeartbeatResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErrorCode;

        public KafkaHeartbeatResponse(KafkaResponseErrorCode errorCode)
        {
            ErrorCode = errorCode;
        }
    }
}
