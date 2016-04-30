using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public enum KafkaConsumerGroupStatus
    {
        NotInitialized = 0,
        Rearrange = 1,
        JoinGroup = 2,      
        Assigning = 3,        
        SyncGroup = 4,
        FirstHeatbeat = 5,
        OffsetsFilling = 6,
        Ready = 7,
        Error = 8,
        Rebalance = 9
    }
}
