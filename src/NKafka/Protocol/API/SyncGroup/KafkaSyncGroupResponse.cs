using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.SyncGroup
{
    [PublicAPI]
    internal sealed class KafkaSyncGroupResponse : IKafkaResponse
    {
        public readonly KafkaResponseErrorCode ErrorCode;

        public readonly short ProtocolVersion;

        public readonly IReadOnlyList<KafkaSyncGroupResponseTopic> AssignedTopics;

        public readonly byte[] CustomData;

        public KafkaSyncGroupResponse(KafkaResponseErrorCode errorCode, short protocolVersion, IReadOnlyList<KafkaSyncGroupResponseTopic> assignedTopics, byte[] customData)
        {
            ErrorCode = errorCode;
            ProtocolVersion = protocolVersion;
            AssignedTopics = assignedTopics;
            CustomData = customData;
        }
    }
}
