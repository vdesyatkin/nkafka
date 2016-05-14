using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerFallbackInfo
    {
        [NotNull] public readonly string TopicName;

        public readonly int PartitionId;

        [NotNull] public readonly KafkaMessage Message;

        public readonly KafkaProducerFallbackErrorCode Reason;

        public KafkaProducerFallbackInfo([NotNull] string topicName, int partitionId,
            [NotNull] KafkaMessage message, KafkaProducerFallbackErrorCode reason)
        {
            TopicName = topicName;
            PartitionId = partitionId;            
            Message = message;
            Reason = reason;
        }
    }

    [PublicAPI]
    public sealed class KafkaProducerFallbackInfo<TKey, TData>
    {
        [NotNull]
        public readonly string TopicName;

        public readonly int PartitionId;        

        [NotNull] public readonly KafkaMessage<TKey, TData> Message;

        public readonly KafkaProducerFallbackErrorCode Reason;

        public KafkaProducerFallbackInfo([NotNull] string topicName, int partitionId,
            [NotNull] KafkaMessage<TKey, TData> message, KafkaProducerFallbackErrorCode reason)
        {
            TopicName = topicName;
            PartitionId = partitionId;            
            Message = message;
            Reason = reason;
        }
    }
}
