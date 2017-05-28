namespace NKafka.Client.ConsumerGroup.Internal
{
    public enum KafkaCoordinatorGroupStatus
    {
        NotInitialized = 0,
        RearrangeRequired = 1,
        JoinGroupRequested = 2,
        JoinedAsMember = 3,
        AdditionalTopicsRequired = 4,
        AdditionalTopicsMetadataRequested = 5,
        JoinedAsLeader = 6,
        SyncGroupRequestedAsLeader = 7,
        SyncGroupRequestedAsMember = 8,
        FirstHeartbeatRequired = 9,
        FirstHeatbeatRequested = 10,
        OffsetFetchRequired = 11,
        OffsetFetchRequested = 12,
        Ready = 13,
        Error = 14,
        Rebalance = 15
    }
}