namespace NKafka.Client.Consumer.Internal
{
    public enum KafkaConsumerBrokerPartitionStatus
    {
        NotInitialized = 0,
        OffsetRequested = 1,
        Plugged = 2,
        RearrangeRequired = 3
    }
}
