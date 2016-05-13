using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly string TopicProducerName;

        [NotNull] public readonly KafkaProducerSettings Settings;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaProducerBrokerPartition> Partitions;

        public int ProducePartitionIndex;

        public KafkaConsistencyLevel ConsistencyLevel;

        public int BatchSizeByteCount;

        [CanBeNull] public IKafkaProducerTopicLogger Logger;

        public KafkaProducerBrokerTopic([NotNull]string topicName, [NotNull]string topicProducerName, [NotNull] KafkaProducerSettings settings)
        {
            TopicName = topicName;
            TopicProducerName = topicProducerName;
            Settings = settings;
            ConsistencyLevel = settings.ConsistencyLevel;
            BatchSizeByteCount = settings.BatchSizeByteCount;
            Partitions = new ConcurrentDictionary<int, KafkaProducerBrokerPartition>();
        }
    }
}
