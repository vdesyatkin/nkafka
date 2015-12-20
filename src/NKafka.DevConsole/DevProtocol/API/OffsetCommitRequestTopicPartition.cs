namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetCommitRequestTopicPartition
    {
        public int PartitionId { get; set; }

        public long Offset { get; set; }

        public string Metadata { get; set; }        
    }
}
