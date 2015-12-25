namespace NKafka.Consumer.Internal
{
    internal enum KafkaConsumerBrokerPartitionStatus
    {
        Unplugged = 0,
        Plugged = 1,
        NeedRearrange = 2,
    }
}
