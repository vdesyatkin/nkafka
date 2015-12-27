using JetBrains.Annotations;

namespace NKafka.Client.Internal
{
    [PublicAPI]
    internal enum KafkaClientBrokerPartitionStatus
    {
        Unplugged = 0,
        Plugged = 1,
        NeedRearrange = 2,
    }
}
