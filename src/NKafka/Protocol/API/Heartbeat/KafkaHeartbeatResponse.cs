using JetBrains.Annotations;

namespace NKafka.Protocol.API.Heartbeat
{
    [PublicAPI]
    internal sealed class KafkaHeartbeatResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErorrCode;

        public KafkaHeartbeatResponse(KafkaResponseErrorCode erorrCode)
        {
            ErorrCode = erorrCode;
        }
    }
}
