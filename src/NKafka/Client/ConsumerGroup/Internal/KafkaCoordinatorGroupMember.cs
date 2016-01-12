using System.Collections.Generic;
using NKafka.Client.ConsumerGroup.Assignment;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupMember
    {
        public readonly string MemberId;

        public readonly bool IsLeader;        

        public readonly short ProtocolVersion;

        public readonly IReadOnlyList<string> AvailableAssignmentStrategies;

        public readonly byte[] CustomData;

        public readonly Dictionary<string, IReadOnlyList<int>> TopicAssignments;

        public KafkaCoordinatorGroupMember(string memberId, bool isLeader, short protocolVersion, IReadOnlyList<string> availableAssignmentStrategies, byte[] customData)
        {
            MemberId = memberId;
            IsLeader = isLeader;            
            ProtocolVersion = protocolVersion;
            AvailableAssignmentStrategies = availableAssignmentStrategies;
            CustomData = customData;
            TopicAssignments = new Dictionary<string, IReadOnlyList<int>>();
        }
    }
}
