using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientTopic
    {
        [NotNull] public readonly string TopicName;

        [NotNull, ItemNotNull] public IReadOnlyList<KafkaClientTopicPartition> Partitions { get; private set; }
        
        public KafkaClientTopicStatus Status;

        [CanBeNull] private readonly KafkaProducerTopic _producer;
        [CanBeNull] private readonly KafkaConsumerTopic _consumer;

        public KafkaClientTopic([NotNull] string topicName, [CanBeNull] KafkaProducerTopic producer, [CanBeNull] KafkaConsumerTopic consumer)
        {
            TopicName = topicName;
            _producer = producer;
            _consumer = consumer;            
            Partitions = new KafkaClientTopicPartition[0];
        }
        
        public void ApplyMetadata([NotNull] KafkaTopicMetadata topicMetadata)
        {
            var partitionBrokers = new Dictionary<int, KafkaBrokerMetadata>(topicMetadata.Brokers.Count);
            foreach (var brokerMetadata in topicMetadata.Brokers)
            {
                partitionBrokers[brokerMetadata.BrokerId] = brokerMetadata;
            }

            var topicName = topicMetadata.TopicName;

            var topicPartitions = new List<KafkaClientTopicPartition>(topicMetadata.Partitions.Count);
            var producerPartitions = new List<KafkaProducerTopicPartition>(topicMetadata.Partitions.Count);
            var consumerPartitions = new List<KafkaConsumerTopicPartition>(topicMetadata.Partitions.Count);
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                var partitionId = partitionMetadata.PartitionId;
                var brokerId = partitionMetadata.LeaderBrokerId;
                KafkaBrokerMetadata brokerMetadata;
                if (!partitionBrokers.TryGetValue(brokerId, out brokerMetadata))
                {
                    continue;
                }

                var producerPartiton = _producer?.CreatePartition(partitionId);
                producerPartitions.Add(producerPartiton);

                var consumerPartition = _consumer?.CreatePartition(partitionId);
                consumerPartitions.Add(consumerPartition);

                var partition = new KafkaClientTopicPartition(topicName, partitionId, brokerMetadata, producerPartiton, consumerPartition);
                topicPartitions.Add(partition);
            }

            Partitions = topicPartitions;
            _producer?.ApplyPartitions(producerPartitions);
            _consumer?.ApplyPartitions(consumerPartitions);
        }

        public void ApplyConsumerCoordinator(IKafkaConsumerCoordinator consumerCoordinator)
        {
            _consumer?.ApplyCoordinator(consumerCoordinator);
        }

        public void Flush()
        {
            _producer?.Flush();            
        }
    }
}
