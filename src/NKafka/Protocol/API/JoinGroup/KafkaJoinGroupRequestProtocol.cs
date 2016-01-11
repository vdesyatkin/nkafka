using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.JoinGroup
{
    [PublicAPI]
    internal sealed class KafkaJoinGroupRequestProtocol
    {
        public readonly string Name;

        public readonly short Version;

        public readonly IReadOnlyList<string> TopicNames;

        public readonly IReadOnlyList<string> AssignmentStrategies;

        public readonly byte[] CustomData;

        public KafkaJoinGroupRequestProtocol(string name, short version, IReadOnlyList<string> topicNames, IReadOnlyList<string> assignmentStrategies, byte[] customData)
        {
            Name = name;
            Version = version;
            TopicNames = topicNames;
            AssignmentStrategies = assignmentStrategies;
            CustomData = customData;
        }
    }
}
