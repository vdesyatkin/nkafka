using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerTopic
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI, NotNull]
        public readonly KafkaConsumerSettings Settings;

        [PublicAPI, NotNull]
        public readonly ConcurrentDictionary<int, KafkaConsumerBrokerPartition> Partitions;

        public KafkaConsumerBrokerTopic([NotNull]string topicName, [NotNull] KafkaConsumerSettings settings)
        {
            TopicName = topicName;
            Settings = settings;
            Partitions = new ConcurrentDictionary<int, KafkaConsumerBrokerPartition>();
        }
    }
}
