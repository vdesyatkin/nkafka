using JetBrains.Annotations;

namespace NKafka.Client.Internal
{   
    [PublicAPI]
    internal enum KafkaClientGroupStatus
    {
        NotInitialized = 0,
        MetadataRequested = 1,
        MetadataError = 2,
        Ready = 3,
        RearrangeRequired = 4,
        Rearrange = 5
    }
}
