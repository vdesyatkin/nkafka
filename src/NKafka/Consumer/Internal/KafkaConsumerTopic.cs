using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Consumer.Internal
{
    internal sealed class KafkaConsumerTopic
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public IReadOnlyList<KafkaConsumerTopicPartition> Partitions { get; private set; }        

        public KafkaConsumerTopicStatus Status;

        [NotNull]
        private readonly IKafkaConsumerTopic _dataConsumer;

        public KafkaConsumerTopic([NotNull] string topicName, [NotNull] IKafkaConsumerTopic dataConsumer)
        {
            TopicName = topicName;
            _dataConsumer = dataConsumer;
            Status = KafkaConsumerTopicStatus.NotInitialized;
            Partitions = new KafkaConsumerTopicPartition[0];
        }

        public void ApplyMetadata([NotNull] KafkaTopicMetadata topicMetadata)
        {
            var partitionBrokers = new Dictionary<int, KafkaBrokerMetadata>(topicMetadata.Brokers.Count);
            foreach (var brokerMetadata in topicMetadata.Brokers)
            {
                partitionBrokers[brokerMetadata.BrokerId] = brokerMetadata;
            }

            var topicName = topicMetadata.TopicName;

            var topicPartitions = new List<KafkaConsumerTopicPartition>(topicMetadata.Partitions.Count);
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                var brokerId = partitionMetadata.LeaderBrokerId;
                KafkaBrokerMetadata brokerMetadata;
                if (!partitionBrokers.TryGetValue(brokerId, out brokerMetadata))
                {
                    continue;
                }
                var partition = new KafkaConsumerTopicPartition(topicName, partitionMetadata.PartitionId, brokerMetadata, _dataConsumer);
                topicPartitions.Add(partition);
            }
            Partitions = topicPartitions;
        }
    }
}
