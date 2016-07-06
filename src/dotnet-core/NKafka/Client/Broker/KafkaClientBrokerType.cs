using JetBrains.Annotations;

namespace NKafka.Client.Broker
{
    [PublicAPI]
    public enum KafkaClientBrokerType
    {
        MessageBroker = 0,
        MetadataBroker = 1
    }
}
