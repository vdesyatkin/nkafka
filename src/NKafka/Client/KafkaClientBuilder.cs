using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.Internal;
using NKafka.Client.Producer;
using NKafka.Client.Producer.Internal;

namespace NKafka.Client
{
    public class KafkaClientBuilder
    {
        [NotNull, ItemNotNull] private readonly List<KafkaProducerTopic> _topicProducers;
        [NotNull, ItemNotNull] private readonly List<KafkaConsumerTopic> _topicConsumers;

        public KafkaClientBuilder()
        {
            _topicProducers = new List<KafkaProducerTopic>();
            _topicConsumers = new List<KafkaConsumerTopic>();
        }

        [PublicAPI]
        public IKafkaProducerTopic CreateTopicProducer([NotNull] string topicName, [NotNull] IKafkaProducerPartitioner partitioner)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topicBuffer = new KafkaProducerTopicBuffer(partitioner);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);
            _topicProducers.Add(topic);            
            return topicBuffer;
        }

        [PublicAPI]
        public IKafkaProducerTopic<TKey, TData> CreateTopicProducer<TKey, TData>([NotNull] string topicName,
           [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
           [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);
            _topicProducers.Add(topic);
            return topicBuffer;
        }

        [PublicAPI]
        public IKafkaConsumerTopic CreateTopicConsumer([NotNull] string topicName)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            if (string.IsNullOrEmpty(topicName)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topic = new KafkaConsumerTopic(topicName);
            _topicConsumers.Add(topic);
            return topic;
        }

        [PublicAPI]
        public IKafkaConsumerTopic<TKey,TData> CreateTopicConsumer<TKey, TData>([NotNull] string topicName,           
           [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            if (string.IsNullOrEmpty(topicName) || (serializer == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topic = new KafkaConsumerTopic(topicName);
            var wrapper = new KafkaConsumerTopicWrapper<TKey, TData>(topic, serializer);
            _topicConsumers.Add(topic);
            return wrapper;
        }

        [PublicAPI, NotNull]
        public IKafkaClient Build([NotNull]KafkaClientSettings settings)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settings = settings ?? new KafkaClientSettingsBuilder(null).Build();

            var topicNames = new HashSet<string>();

            var producers = new Dictionary<string, KafkaProducerTopic>(_topicProducers.Count);
            foreach (var producer in _topicProducers)
            {
                topicNames.Add(producer.TopicName);
                producers[producer.TopicName] = producer;
            }

            var consumers = new Dictionary<string, KafkaConsumerTopic>(_topicConsumers.Count);
            foreach (var consumer in _topicConsumers)
            {
                topicNames.Add(consumer.TopicName);
                consumers[consumer.TopicName] = consumer;
            }

            var topics = new List<KafkaClientTopic>(topicNames.Count);
            foreach (var topicName in topicNames)
            {
                KafkaProducerTopic producer;
                producers.TryGetValue(topicName, out producer);

                KafkaConsumerTopic consumer;
                consumers.TryGetValue(topicName, out consumer);

                var topic = new KafkaClientTopic(topicName, producer, consumer);
                topics.Add(topic);
            }

            return new KafkaClient(settings, topics);
        }

        [PublicAPI, NotNull]
        public IKafkaClient Build([NotNull]KafkaClientSettingsBuilder settingsBuilder)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settingsBuilder = settingsBuilder ?? new KafkaClientSettingsBuilder(null);

            return Build(settingsBuilder.Build());
        }
    }
}
