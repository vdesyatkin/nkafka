using System;
using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Metadata;

namespace NKafka.Consumer.Internal
{
    internal sealed class KafkaConsumerBroker
    {
        public bool IsEnabled => _broker.IsEnabled;
        public bool IsOpenned => _broker.IsOpenned;

        [NotNull] private readonly KafkaBroker _broker;

        [NotNull] private readonly ConcurrentDictionary<string, KafkaConsumerBrokerTopic> _topics;        

        private readonly int _batchByteCountLimit;
        private readonly TimeSpan _consumeOnServerTimeout;
        private readonly TimeSpan _consumeTotalTimeout;        

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, [NotNull] KafkaConsumerSettings settings)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();
            _batchByteCountLimit = settings.BatchByteCountLimit;
            _consumeOnServerTimeout = settings.ConsumeTimeout;
            if (_consumeOnServerTimeout < TimeSpan.FromSeconds(1))
            {
                _consumeOnServerTimeout = TimeSpan.FromSeconds(1); //todo (E006) settings server-side validation
            }
            _consumeTotalTimeout = _consumeOnServerTimeout +
                                   TimeSpan.FromMilliseconds(settings.ConsumePeriod.TotalMilliseconds*2) +
                                   TimeSpan.FromSeconds(1);

            //todo (E006) settings client-side validation
        }

        public void Maintenance()
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
                        partition.Status = KafkaConsumerBrokerPartitionStatus.Unplugged;
                        topic.Partitions.TryRemove(partitionPair.Key, out partition);
                        continue;
                    }

                    if (partition.Status == KafkaConsumerBrokerPartitionStatus.Unplugged)
                    {
                        partition.Status = KafkaConsumerBrokerPartitionStatus.Plugged;
                    }
                }
            }
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
                    partition.Value.Status = KafkaConsumerBrokerPartitionStatus.Unplugged;
                }
                topic.Value.Partitions.Clear();
            }
            _broker.Close();
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaConsumerBrokerTopic(topicName),
                    (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public KafkaBrokerResult<int?> RequestTopicMetadata(string topicName)
        {
            return (int?)null;
        }

        public KafkaBrokerResult<KafkaTopicMetadata> GetTopicMetadata(int requestId)
        {
            return (KafkaTopicMetadata)null;
        }

        public void PerformConsume()
        {            
        }
    }
}
