using System.Collections.Generic;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupMember
    {
        public readonly string MemberId;

        public readonly bool IsLeader;

        public readonly string ProtocolName;

        public readonly short ProtocolVersion;

        public readonly IReadOnlyList<string> AvailableAssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaCoordinatorGroupMember(string memberId, bool isLeader, string protocolName, short protocolVersion, IReadOnlyList<string> availableAssignmentStrategies, byte[] customData)
        {
            MemberId = memberId;
            IsLeader = isLeader;
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            AvailableAssignmentStrategies = availableAssignmentStrategies;
            CustomData = customData;
        }
    }
}
