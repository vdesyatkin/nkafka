using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public class KafkaBrokerMetadata
    {
        public readonly int BrokerId;
        public readonly string Host;
        public readonly int Port;

        public KafkaBrokerMetadata(int brokerId, string host, int port)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
        }
    }
}
