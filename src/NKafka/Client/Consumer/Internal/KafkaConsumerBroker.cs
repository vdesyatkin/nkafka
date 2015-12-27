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
        private readonly TimeSpan _consumeServerTimeout;
        private readonly TimeSpan _consumeClientTimeout;

        public KafkaConsumerBroker([NotNull] KafkaBroker broker, TimeSpan consumePeriod, [NotNull] KafkaConsumerSettings settings)
        {
            _broker = broker;
            _topics = new ConcurrentDictionary<string, KafkaConsumerBrokerTopic>();
            _batchMinSizeBytes = settings.ConsumeBatchMinSizeBytes;            
            _consumeServerTimeout = settings.ConsumeServerTimeout;
            if (_consumeServerTimeout < TimeSpan.Zero)
            {
                _consumeServerTimeout = TimeSpan.Zero; //todo (E006) settings server-side validation
            }

            _consumeClientTimeout = _consumeServerTimeout +
                                   TimeSpan.FromMilliseconds(consumePeriod.TotalMilliseconds * 2) +
                                   TimeSpan.FromSeconds(1);
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

        public void Consume()
        {
            //todo (C005) fetch request
        }
    }
}
