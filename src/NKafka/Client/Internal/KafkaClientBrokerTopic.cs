using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientBrokerTopic
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI, NotNull]
        public readonly ConcurrentDictionary<int, KafkaClientBrokerPartition> Partitions;

        public KafkaClientBrokerTopic([NotNull]string topicName)
        {
            TopicName = topicName;
            Partitions = new ConcurrentDictionary<int, KafkaClientBrokerPartition>();
        }
    }
}
