namespace NKafka.Metadata
{
    internal sealed class KafkaTopicPartitionMetadata
    {
        public readonly int PartitionId;

        public readonly int LeaderBrokerId;

        public KafkaTopicPartitionMetadata(int partitionId, int leaderBrokerId)
        {
            PartitionId = partitionId;
            LeaderBrokerId = leaderBrokerId;
        }
    }
}
