using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Diagnostics;
using NKafka.Client.Producer.Internal;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientTopic
    {
        [NotNull] public readonly string TopicName;

        [CanBeNull] private string _clusterId;

        [NotNull, ItemNotNull] public IReadOnlyList<KafkaClientTopicPartition> Partitions { get; private set; }
        
        public KafkaClientTopicStatus Status;

        [CanBeNull] public readonly KafkaProducerTopic Producer;
        [CanBeNull] public readonly KafkaConsumerTopic Consumer;

        [NotNull] public KafkaClientTopicMetadataInfo MetadataInfo => _metadataInfo;
        [NotNull] private KafkaClientTopicMetadataInfo _metadataInfo;

        public KafkaClientTopic([NotNull] string topicName, [CanBeNull] KafkaProducerTopic producer, [CanBeNull] KafkaConsumerTopic consumer)
        {
            TopicName = topicName;
            Producer = producer;
            Consumer = consumer;
            Partitions = new KafkaClientTopicPartition[0];
            _metadataInfo = new KafkaClientTopicMetadataInfo(topicName, null, false, null, null, DateTime.UtcNow);
        }

        public void ChangeMetadataState(bool isReady, KafkaClientTopicMetadataErrorCode? errorCode, [CanBeNull] KafkaTopicMetadata metadata)
        {
            _metadataInfo = new KafkaClientTopicMetadataInfo(TopicName, _clusterId, isReady, errorCode, metadata, DateTime.UtcNow);
            if (isReady && metadata != null)
            {
                ApplyMetadata(metadata);
            }

            var producer = Producer;
            if (producer != null)
            {
                producer.TopicMetadataInfo = _metadataInfo;
            }

            var consumer = Consumer;
            if (consumer != null)
            {
                consumer.TopicMetadataInfo = _metadataInfo;
            }
        }
        
        private void ApplyMetadata([NotNull] KafkaTopicMetadata topicMetadata)
        {
            _clusterId = topicMetadata.ClusterId;

            var partitionBrokers = new Dictionary<int, KafkaBrokerMetadata>(topicMetadata.Brokers.Count);
            foreach (var brokerMetadata in topicMetadata.Brokers)
            {
                partitionBrokers[brokerMetadata.BrokerId] = brokerMetadata;
            }            

            var oldPartitions = new Dictionary<int, KafkaClientTopicPartition>(Partitions.Count);
            foreach (var partition in Partitions)
            {
                oldPartitions[partition.PartitionId] = partition;
            }

            var topicPartitions = new List<KafkaClientTopicPartition>(topicMetadata.Partitions.Count);
            var producerPartitions = new List<KafkaProducerTopicPartition>(topicMetadata.Partitions.Count);
            var consumerPartitions = new List<KafkaConsumerTopicPartition>(topicMetadata.Partitions.Count);
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                var partitionId = partitionMetadata.PartitionId;
                var brokerId = partitionMetadata.LeaderBrokerId;
                KafkaBrokerMetadata brokerMetadata;
                if (!partitionBrokers.TryGetValue(brokerId, out brokerMetadata) || brokerMetadata == null)
                {
                    continue;
                }

                KafkaProducerTopicPartition producerPartition;
                KafkaConsumerTopicPartition consumerPartition;

                KafkaClientTopicPartition oldPartition;
                if (oldPartitions.TryGetValue(partitionId, out oldPartition) && oldPartition != null)
                {
                    producerPartition = oldPartition.ProducerPartition;
                    consumerPartition = oldPartition.ConsumerPartition;
                }
                else
                {
                    producerPartition = Producer?.CreatePartition(partitionId);
                    consumerPartition = Consumer?.CreatePartition(partitionId);
                }

                if (producerPartition != null)
                {
                    producerPartitions.Add(producerPartition);
                }
                
                if (consumerPartition != null)
                {
                    consumerPartitions.Add(consumerPartition);
                }

                var partition = new KafkaClientTopicPartition(TopicName, partitionId, brokerMetadata, producerPartition, consumerPartition);
                topicPartitions.Add(partition);
            }

            Partitions = topicPartitions;
            Producer?.ApplyPartitions(producerPartitions);
            Consumer?.ApplyPartitions(consumerPartitions);
        }

        public void DistributeMessages()
        {
            Producer?.DistributeMessagesByPartitions();
        }
    }
}
