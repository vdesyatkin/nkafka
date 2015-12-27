using System;
using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;

namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientBroker
    {
        public bool IsEnabled => _broker.IsEnabled;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull]
        private readonly KafkaBroker _broker;

        [NotNull]
        private readonly ConcurrentDictionary<string, KafkaClientBrokerTopic> _topics;        

        public KafkaClientBroker([NotNull] KafkaBroker broker, [NotNull] KafkaClientSettings settings)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaClientBrokerTopic>();          
        }

        public void Work()
        {
            _broker.Maintenance();

            foreach (var topicPair in _topics)
            {
                var topic = topicPair.Value;
                foreach (var partitionPair in topic.Partitions)
                {
                    var partition = partitionPair.Value;
                    if (partition.IsUnplugRequired)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.Unplugged;
                        topic.Partitions.TryRemove(partitionPair.Key, out partition);
                        continue;
                    }

                    if (partition.Status == KafkaClientBrokerPartitionStatus.Unplugged)
                    {
                        partition.Status = KafkaClientBrokerPartitionStatus.Plugged;
                    }
                }
            }
            
            //todo (C002) producer borker work
        }

        public void Open()
        {
            _broker.Open();
        }

        public void Close()
        {
            foreach (var topic in _topics)
            {
                foreach (var partition in topic.Value.Partitions)
                {
                    partition.Value.Status = KafkaClientBrokerPartitionStatus.Unplugged;
                }
                topic.Value.Partitions.Clear();
            }
            _broker.Close();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaClientBrokerPartition topicPartition)
        {
            KafkaClientBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaClientBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public KafkaBrokerResult<int?> RequestTopicMetadata([NotNull] string topicName)
        {
            return _broker.RequestTopicMetadata(topicName, TimeSpan.FromSeconds(5)); //todo (C002) which settings should I use?
        }

        public KafkaBrokerResult<KafkaTopicMetadata> GetTopicMetadata(int requestId)
        {
            return _broker.GetTopicMetadata(requestId);
        }
    }
}
