using JetBrains.Annotations;

namespace NKafka.Protocol.API.Heartbeat
{
    [PublicAPI]
    public sealed class KafkaHeartbeatRequest : IKafkaRequest
    {
        public readonly string GroupId;

        public readonly int GroupGenerationId;

        public readonly string MemberId;

        public KafkaHeartbeatRequest(string groupId, int groupGenerationId, string memberId)
        {
            GroupId = groupId;
            GroupGenerationId = groupGenerationId;
            MemberId = memberId;
        }
    }
}
