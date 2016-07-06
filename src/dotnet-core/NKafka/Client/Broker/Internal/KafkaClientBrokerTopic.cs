using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Broker.Internal
{
    internal sealed class KafkaClientBrokerTopic
    {
        [NotNull] public readonly string TopicName;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaClientBrokerPartition> Partitions;

        public KafkaClientBrokerTopic([NotNull]string topicName)
        {
            TopicName = topicName;
            Partitions = new ConcurrentDictionary<int, KafkaClientBrokerPartition>();
        }
    }
}
