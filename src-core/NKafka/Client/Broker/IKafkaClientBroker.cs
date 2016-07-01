using JetBrains.Annotations;
using NKafka.Client.Broker.Diagnostics;
using NKafka.Metadata;

namespace NKafka.Client.Broker
{
    [PublicAPI]
    public interface IKafkaClientBroker
    {
        [NotNull] string Name { get; }
        int WorkerId { get; }
        KafkaClientBrokerType BrokerType { get; }
        [NotNull] KafkaBrokerMetadata BrokerMetadata { get; }        

        bool IsStarted { get; }
        bool IsEnabled { get; }
        [NotNull] KafkaClientBrokerInfo GetDiagnosticsInfo();
    }
}
