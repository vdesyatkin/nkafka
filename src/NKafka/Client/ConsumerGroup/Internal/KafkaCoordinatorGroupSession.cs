using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupSession
    {
        public readonly DateTime TimestampUtc;

        public readonly int GenerationId;

        [CanBeNull] public readonly string MemberId;

        public readonly bool IsLeader;

        [CanBeNull] public readonly string ProtocolName;

        public KafkaCoordinatorGroupSession(DateTime timestampUtc, int generationId, string memberId, bool isLeader, string protocolName)
        {
            TimestampUtc = timestampUtc;
            GenerationId = generationId;
            MemberId = memberId;
            IsLeader = isLeader;
            ProtocolName = protocolName;
        }
    }
}
