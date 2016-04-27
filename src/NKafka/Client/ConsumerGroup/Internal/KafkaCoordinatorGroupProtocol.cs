using System;

namespace NKafka.Client.ConsumerGroup.Internal
{
    internal sealed class KafkaCoordinatorGroupProtocol
    {
        public readonly string ProtocolName;

        public readonly short? ProtocolVersion;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupProtocol(string protocolName, short? protocolVersion, DateTime timestampUtc)
        {
            ProtocolName = protocolName;
            ProtocolVersion = protocolVersion;
            TimestampUtc = timestampUtc;
        }
    }
}
