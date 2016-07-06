using JetBrains.Annotations;

namespace NKafka.Metadata
{
    [PublicAPI]
    public sealed class KafkaTopicPartitionMetadata
    {
        public readonly int PartitionId;

        public readonly KafkaTopicPartitionMetadataErrorCode? Error;

        public readonly int LeaderBrokerId;

        public KafkaTopicPartitionMetadata(int partitionId, 
            KafkaTopicPartitionMetadataErrorCode? error, 
            int leaderBrokerId)
        {
            PartitionId = partitionId;
            Error = error;
            LeaderBrokerId = leaderBrokerId;
        }
    }
}
