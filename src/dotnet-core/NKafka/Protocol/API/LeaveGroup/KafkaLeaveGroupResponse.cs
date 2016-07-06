using JetBrains.Annotations;

namespace NKafka.Protocol.API.LeaveGroup
{
    [PublicAPI]
    public sealed class KafkaLeaveGroupResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErrorCode;

        public KafkaLeaveGroupResponse(KafkaResponseErrorCode errorCode)
        {
            ErrorCode = errorCode;
        }
    }
}
