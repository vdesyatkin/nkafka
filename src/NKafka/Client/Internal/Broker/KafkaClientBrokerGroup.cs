using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Internal.Broker
{
    internal sealed class KafkaClientBrokerGroup
    {
        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        public bool IsUnplugRequired;

        public KafkaClientBrokerGroupStatus Status;

        public KafkaClientBrokerGroup([NotNull] KafkaBrokerMetadata brokerMetadata)
        {
            BrokerMetadata = brokerMetadata;
        }
    }
}
