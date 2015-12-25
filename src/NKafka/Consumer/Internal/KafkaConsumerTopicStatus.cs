namespace NKafka.Consumer.Internal
{
    internal enum KafkaConsumerTopicStatus
    {
        NotInitialized = 0,
        MetadataRequested = 1,
        Ready = 2,
        RearrangeRequired = 3
    }
}
