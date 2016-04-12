using JetBrains.Annotations;

namespace NKafka.Client.Internal
{
    [PublicAPI]
    internal enum KafkaClientTopicStatus : byte
    {        
        NotInitialized = 0,
        MetadataRequested = 1,
        MetadataError = 2,
        Ready = 3,
        RearrangeRequired = 4
    }
}
