using JetBrains.Annotations;

namespace NKafka
{
    public sealed class KafkaBrokerInfo
    {
        public readonly string Host;
        public readonly int Port;

        public KafkaBrokerInfo([NotNull] string host, int port)
        {
            Host = host;
            Port = port;
        }
    }
}
