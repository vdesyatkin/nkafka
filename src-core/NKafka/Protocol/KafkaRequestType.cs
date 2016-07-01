using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    public enum KafkaRequestType : short
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        TopicMetadata = 3,        

        OffsetCommit = 8,
        OffsetFetch = 9,
        GroupCoordinator = 10,

        JoinGroup = 11,
        Heartbeat = 12,
        LeaveGroup = 13,
        SyncGroup = 14        
    }
}
