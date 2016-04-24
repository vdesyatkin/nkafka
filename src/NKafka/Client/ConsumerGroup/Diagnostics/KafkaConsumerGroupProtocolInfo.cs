using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupProtocolInfo
    {
        public readonly string ProtocolName;

        public readonly short ProtocolVersion;

        public readonly string AssignmentStrategyName;

        public readonly byte[] CustomData;

        public KafkaConsumerGroupProtocolInfo(string protocolName, short protocolVersion, string assignmentStrategyName, byte[] customData)
        {
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            AssignmentStrategyName = assignmentStrategyName;
        }
    }
}
