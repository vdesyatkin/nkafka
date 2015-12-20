using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Producer.Internal
{
    internal sealed class KafkaProducerBrokerPartition
    {
        public readonly string TopicName;

        public readonly int PartitionId;

        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        [NotNull] public readonly IKafkaProducerMessageQueue Queue;

        public bool IsUnplugRequired;

        public KafkaProducerBrokerPartitionStatus Status;

        public KafkaProducerBrokerPartition(string topicName, int partitionId, [NotNull] KafkaBrokerMetadata brokerMetadata, [NotNull] IKafkaProducerMessageQueue queue)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            BrokerMetadata = brokerMetadata;
            Queue = queue;
        }
    }
}
