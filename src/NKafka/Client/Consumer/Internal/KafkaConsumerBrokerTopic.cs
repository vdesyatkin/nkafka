using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly IKafkaConsumerCoordinator Coordinator;

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaConsumerBrokerPartition> Partitions;

        public KafkaConsumerBrokerTopic([NotNull]string topicName, [NotNull] KafkaConsumerSettings settings,
            [NotNull] IKafkaConsumerCoordinator coordinator)
        {
            TopicName = topicName;
            Settings = settings;
            Coordinator = coordinator;
            Partitions = new ConcurrentDictionary<int, KafkaConsumerBrokerPartition>();
        }
    }
}
