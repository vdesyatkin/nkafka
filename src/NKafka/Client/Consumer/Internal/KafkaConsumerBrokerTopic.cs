using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerTopic
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly string TopicConsumerName;
        [NotNull] public readonly KafkaConsumerGroupData Group;        

        [NotNull] public readonly KafkaConsumerSettings Settings;

        [NotNull] public readonly ConcurrentDictionary<int, KafkaConsumerBrokerPartition> Partitions;

        public KafkaConsumerBrokerTopic([NotNull]string topicName, [NotNull]string topicConsumerName, [NotNull] KafkaConsumerGroupData group,
            [NotNull] KafkaConsumerSettings settings)
        {
            TopicName = topicName;
            TopicConsumerName = topicConsumerName;
            Settings = settings;
            Group = group;            
            Partitions = new ConcurrentDictionary<int, KafkaConsumerBrokerPartition>();
        }
    }
}
