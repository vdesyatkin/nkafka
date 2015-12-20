namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetCommitResponseTopicPartition
    {
        public int PartitionId { get; set; }
        public ErrorResponseCode Error { get; set; }
    }
}
