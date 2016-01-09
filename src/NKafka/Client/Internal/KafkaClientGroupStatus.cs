using JetBrains.Annotations;

namespace NKafka.Client.Internal
{   
    [PublicAPI]
    internal enum KafkaClientGroupStatus : byte
    {
        NotInitialized = 0,
        MetadataRequested = 1,
        Ready = 2,
        RearrangeRequired = 3
    }
}
