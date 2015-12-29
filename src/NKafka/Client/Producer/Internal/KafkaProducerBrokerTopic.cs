using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerTopic
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI, NotNull]
        public readonly KafkaProducerSettings Settings;

        [PublicAPI, NotNull]
        public readonly ConcurrentDictionary<int, KafkaProducerBrokerPartition> Partitions;

        public int ProducePartitionIndex;      

        public KafkaProducerBrokerTopic([NotNull]string topicName, [NotNull] KafkaProducerSettings settings)
        {
            TopicName = topicName;
            Settings = settings;
            Partitions = new ConcurrentDictionary<int, KafkaProducerBrokerPartition>();
        }
    }
}
