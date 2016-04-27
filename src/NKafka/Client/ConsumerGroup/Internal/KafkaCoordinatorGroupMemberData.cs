using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupMemberData
    {
        [NotNull] public readonly string MemberId;

        public readonly bool IsLeader;        

        public readonly short ProtocolVersion;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<string> SupportedAssignmentStrategies;

        [CanBeNull] public readonly byte[] CustomData;

        [NotNull] public readonly Dictionary<string, IReadOnlyList<int>> TopicAssignments;

        public KafkaCoordinatorGroupMemberData([NotNull] string memberId, bool isLeader, short protocolVersion, 
            [NotNull, ItemNotNull] IReadOnlyList<string> supportedAssignmentStrategies, [CanBeNull] byte[] customData)
        {
            MemberId = memberId;
            IsLeader = isLeader;            
            ProtocolVersion = protocolVersion;
            SupportedAssignmentStrategies = supportedAssignmentStrategies;
            CustomData = customData;
            TopicAssignments = new Dictionary<string, IReadOnlyList<int>>();
        }
    }
}
