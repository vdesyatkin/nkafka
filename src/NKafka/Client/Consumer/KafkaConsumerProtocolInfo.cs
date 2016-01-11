using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerProtocolInfo
    {
        public readonly string ProtocolName;

        public readonly short ProtocolVersion;        

        public readonly IReadOnlyList<KafkaConsumerAssignmentStrategyInfo> AssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaConsumerProtocolInfo([NotNull] string protocolName, short protocolVersion,
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
