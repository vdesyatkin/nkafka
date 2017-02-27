using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public sealed class KafkaBrokerMetadata
    {
        public readonly int BrokerId;

        public readonly string Host;

        public readonly int Port;

        public readonly string Rack;

        public KafkaBrokerMetadata(int brokerId, string host, int port, string rack)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
            Rack = rack;
        }
    }
}
