namespace NKafka.Producer.Internal
{
    internal enum KafkaProducerTopicStatus
    {
        NotInitialized = 0,        
        MetadataRequested = 1,        
        Ready = 2,
        RearrangeRequired = 3
    }
}
