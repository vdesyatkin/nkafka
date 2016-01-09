using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerTopic
    {
        [NotNull] public readonly string TopicName;

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaConsumerBrokerPartition> Partitions;

        public KafkaConsumerBrokerTopic([NotNull]string topicName, [NotNull] KafkaConsumerSettings settings)
        {
            TopicName = topicName;
            Settings = settings;
            Partitions = new ConcurrentDictionary<int, KafkaConsumerBrokerPartition>();
        }
    }
}
