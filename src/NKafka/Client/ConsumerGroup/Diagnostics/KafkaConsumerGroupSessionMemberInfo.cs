using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupSessionMemberInfo
    {
        public readonly int? GenerationId;

        public readonly string MemberId;

        public readonly bool IsLeader;

        public KafkaConsumerGroupSessionMemberInfo(int? generationId, string memberId, bool isLeader)
        {
            GenerationId = generationId;
            MemberId = memberId;
            IsLeader = isLeader;
        }
    }
}
