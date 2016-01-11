using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.JoinGroup
{
    [PublicAPI]
    internal sealed class KafkaJoinGroupResponseMember
    {
        public readonly string MemberId;

        public readonly short ProtocolVersion;

        public readonly IReadOnlyList<string> TopicNames;

        public readonly IReadOnlyList<string> AssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaJoinGroupResponseMember(string memberId, short protocolVersion, IReadOnlyList<string> topicNames, IReadOnlyList<string> assignmentStrategies, byte[] customData)
        {
            MemberId = memberId;
            ProtocolVersion = protocolVersion;
            TopicNames = topicNames;
            AssignmentStrategies = assignmentStrategies;
            CustomData = customData;
        }
    }
}
