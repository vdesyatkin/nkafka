using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public sealed class KafkaConsumerFallbackInfo
    {
        [NotNull] public readonly string TopicName;

        public readonly int PartitionId;

        public readonly KafkaConsumerFallbackErrorCode Reason;

        public readonly long ClientCommitOffset;

        public readonly long? ServerCommitOffset;

        public bool IsHandled;

        public KafkaConsumerFallbackInfo([NotNull] string topicName, int partitionId,
            KafkaConsumerFallbackErrorCode reason,
            long clientCommitOffset, long? serverCommitOffset)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            Reason = reason;
            ClientCommitOffset = clientCommitOffset;
            ServerCommitOffset = serverCommitOffset;
        }
    }
}