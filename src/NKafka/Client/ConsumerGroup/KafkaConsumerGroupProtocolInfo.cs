using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Assignment;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupProtocolInfo
    {
        public readonly string ProtocolName;

        public readonly short ProtocolVersion;        

        public readonly IReadOnlyList<KafkaConsumerAssignmentStrategyInfo> AssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaConsumerGroupProtocolInfo([NotNull] string protocolName, short protocolVersion,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerAssignmentStrategyInfo> assignmentStrategies,
            [CanBeNull] byte[] customData)
        {
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            AssignmentStrategies = assignmentStrategies;
            CustomData = customData;
        }
    }
}
