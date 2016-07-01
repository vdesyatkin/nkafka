namespace NKafka.Client.Consumer.Internal
{
    public enum KafkaConsumerBrokerPartitionStatus
    {
        NotInitialized = 0,
        RearrangeRequired = 1,
        OffsetRequested = 2,
        Ready = 3,
        Error = 4
    }
}
