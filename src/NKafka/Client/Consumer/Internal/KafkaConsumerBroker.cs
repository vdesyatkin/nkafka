using System;
using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Connection;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBroker
    {
        [NotNull] private readonly KafkaBroker _broker;
        [NotNull] private readonly ConcurrentDictionary<string, KafkaConsumerBrokerTopic> _topics;      

        private readonly int _batchMinSizeBytes;        
        private readonly TimeSpan _consumeOnServerTimeout;        

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, [NotNull] KafkaConsumerSettings settings)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();
            _batchMinSizeBytes = settings.ConsumeBatchMinSizeBytes;            
            _consumeOnServerTimeout = settings.ConsumeTimeout;
            if (_consumeOnServerTimeout < TimeSpan.Zero)
            {
                _consumeOnServerTimeout = TimeSpan.Zero; //todo (E006) settings server-side validation
            }            
        }

        public void AddTopicPartition([NotNull] string topicName, [NotNull] KafkaConsumerBrokerPartition topicPartition)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                topic = _topics.AddOrUpdate(topicName, new KafkaConsumerBrokerTopic(topicName), (oldKey, oldValue) => oldValue);
            }

            topic.Partitions[topicPartition.PartitionId] = topicPartition;
        }

        public void RemoveTopicPartition([NotNull] string topicName, int partitionId)
        {
            KafkaConsumerBrokerTopic topic;
            if (!_topics.TryGetValue(topicName, out topic))
            {
                return;
            }

            KafkaConsumerBrokerPartition partition;
            topic.Partitions.TryRemove(partitionId, out partition);
        }

        public void Work()
        {
            //todo (C001)
        }
    }
}
