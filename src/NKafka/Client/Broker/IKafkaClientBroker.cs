using JetBrains.Annotations;
using NKafka.Client.Diagnostics;
using NKafka.Metadata;

namespace NKafka.Client.Broker
{
    public interface IKafkaClientBroker
    {
        [NotNull] string Name { get; }
        KafkaClientBrokerType BrokerType { get; }
        [NotNull] KafkaBrokerMetadata BrokerMetadata { get; }

        bool IsStarted { get; }
        bool IsEnabled { get; }
        [NotNull] KafkaClientBrokerInfo GetDiagnosticsInfo();
    }
}
