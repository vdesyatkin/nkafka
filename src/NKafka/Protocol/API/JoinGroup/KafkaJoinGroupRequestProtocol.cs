using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.JoinGroup
{
    [PublicAPI]
    public sealed class KafkaJoinGroupRequestProtocol
    {
        public readonly string ProtocolName;

        public readonly short ProtocolVersion;

        public readonly IReadOnlyList<string> TopicNames;

        public readonly IReadOnlyList<string> AssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaJoinGroupRequestProtocol(string protocolName, short protocolVersion, IReadOnlyList<string> topicNames, IReadOnlyList<string> assignmentStrategies, byte[] customData)
        {
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            TopicNames = topicNames;
            AssignmentStrategies = assignmentStrategies;
            CustomData = customData;
        }
    }
}
