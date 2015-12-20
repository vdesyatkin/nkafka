namespace NKafka.Producer.Internal
{
    internal enum KafkaProducerBrokerPartitionStatus
    {
        Unplugged = 0,
        Plugged = 1,
        NeedRearrange = 2,
    }
}