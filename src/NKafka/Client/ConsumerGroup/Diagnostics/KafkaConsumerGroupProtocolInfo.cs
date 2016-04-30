using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupProtocolInfo
    {
        public readonly string ProtocolName;

        public readonly short? ProtocolVersion;

        [CanBeNull] public readonly string AssignmentStrategyName;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupProtocolInfo(string protocolName, short? protocolVersion, string assignmentStrategyName, DateTime timestampUtc)
        {
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            AssignmentStrategyName = assignmentStrategyName;
            TimestampUtc = timestampUtc;
        }
    }
}
