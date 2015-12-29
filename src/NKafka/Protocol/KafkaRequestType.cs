using JetBrains.Annotations;

namespace NKafka.Protocol
{
    [PublicAPI]
    internal enum KafkaRequestType : short
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        TopicMetadata = 3,

        LeaderAndIsrs = 4,
        StopReplica = 5,
        UpdateMetadata = 6,
        ControledShutdown = 7,

        OffsetCommit = 8,
        OffsetFetch = 9,
        GroupCoordinator = 10,

        JoinGroup = 11,
        Heartbeat = 12,
        LeaveGroup = 13,
        SyncGroup = 14,

        DescribeGroups = 15,
        ListGroups = 16
    }
}
