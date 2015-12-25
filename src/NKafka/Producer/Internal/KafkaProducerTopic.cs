﻿using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Producer.Internal
{
    internal class KafkaProducerTopic
    {     
        [NotNull] public readonly string TopicName;        

        [NotNull] public IReadOnlyList<KafkaProducerTopicPartition> Partitions { get; private set; }

        public KafkaProducerTopicStatus Status;

        [NotNull]
        private readonly IKafkaProducerTopicBuffer _buffer;

        public KafkaProducerTopic([NotNull] string topicName, [NotNull] IKafkaProducerTopicBuffer buffer)
        {
            TopicName = topicName;
            _buffer = buffer;
            Status = KafkaProducerTopicStatus.NotInitialized;
            Partitions = new KafkaProducerTopicPartition[0];
        }        

        public void Flush()
        {
            _buffer.Flush(Partitions);
        }

        public void ApplyMetadata([NotNull] KafkaTopicMetadata topicMetadata)
        {
            var partitionBrokers = new Dictionary<int, KafkaBrokerMetadata>(topicMetadata.Brokers.Count);
            foreach (var brokerMetadata in topicMetadata.Brokers)
            {
                partitionBrokers[brokerMetadata.BrokerId] = brokerMetadata;
            }

            var topicName = topicMetadata.TopicName;

            var topicPartitions = new List<KafkaProducerTopicPartition>(topicMetadata.Partitions.Count);
            foreach (var partitionMetadata in topicMetadata.Partitions)
            {
                var brokerId = partitionMetadata.LeaderBrokerId;
                KafkaBrokerMetadata brokerMetadata;
                if (!partitionBrokers.TryGetValue(brokerId, out brokerMetadata))
                {
                    continue;
                }
                var partition = new KafkaProducerTopicPartition(topicName, partitionMetadata.PartitionId, brokerMetadata);
                topicPartitions.Add(partition);
            }

            Partitions = topicPartitions;
        }
    }  
}
