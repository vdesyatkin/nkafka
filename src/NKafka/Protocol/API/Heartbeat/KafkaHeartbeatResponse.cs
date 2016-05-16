using JetBrains.Annotations;

namespace NKafka.Protocol.API.Heartbeat
{
    [PublicAPI]
    public sealed class KafkaHeartbeatResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErrorCode;

        public KafkaHeartbeatResponse(KafkaResponseErrorCode errorCode)
        {
            ErrorCode = errorCode;
        }
    }
}
