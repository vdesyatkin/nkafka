using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerTopic
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly ConcurrentDictionary<int, KafkaProducerBrokerPartition> Partitions;

        public KafkaProducerBrokerTopic([NotNull]string topicName)
        {
            TopicName = topicName;
            Partitions = new ConcurrentDictionary<int, KafkaProducerBrokerPartition>();
        }
    }
}
