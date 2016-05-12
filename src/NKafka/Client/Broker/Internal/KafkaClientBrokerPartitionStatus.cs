using JetBrains.Annotations;

namespace NKafka.Client.Broker.Internal
{
    [PublicAPI]
    internal enum KafkaClientBrokerPartitionStatus
    {
        Unplugged = 0,      
        Plugged = 1,
        RearrangeRequired = 2,
    }
}
