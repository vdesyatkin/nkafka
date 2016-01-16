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
        SyncGroupRequested = 7,
        FirstHeatbeatRequired = 8,
        FirstHeatbeatRequested = 9,
        OffsetFetchRequired = 10,
        OffsetFetchRequested = 11,
        Ready = 12
    }
}
