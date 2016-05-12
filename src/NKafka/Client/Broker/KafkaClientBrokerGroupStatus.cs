using JetBrains.Annotations;

namespace NKafka.Client.Broker
{    
    [PublicAPI]
    internal enum KafkaClientBrokerGroupStatus
    {
        Unplugged = 0,
        Plugged = 1,
        RearrangeRequired = 2
    }
}
