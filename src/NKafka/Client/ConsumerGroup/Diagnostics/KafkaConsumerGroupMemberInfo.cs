using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupMemberInfo
    {
        public readonly int? GenerationId;

        public readonly string MemberId;

        public readonly bool IsLeader;

        public KafkaConsumerGroupMemberInfo(int? generationId, string memberId, bool isLeader)
        {
            GenerationId = generationId;
            MemberId = memberId;
            IsLeader = isLeader;
        }
    }
}
