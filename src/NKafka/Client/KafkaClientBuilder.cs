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
        [NotNull] private readonly KafkaClientSettings _settings;

        public KafkaClientBuilder([NotNull]KafkaClientSettings settings)
        {
            _topicProducers = new List<KafkaProducerTopic>();
            _topicConsumers = new List<KafkaConsumerTopic>();
            // ReSharper disable once ConstantNullCoalescingCondition
            _settings = settings ?? new KafkaClientSettingsBuilder(null).Build();
        }

        [PublicAPI]
        public IKafkaProducerTopic CreateTopicProducer([NotNull] string topicName, 
            [NotNull] KafkaProducerSettings settings,
            [NotNull] IKafkaProducerPartitioner partitioner)
        {            
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;
            settings = settings ?? new KafkaProducerSettingsBuilder().Build();
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer(partitioner);
            var topic = new KafkaProducerTopic(topicName, settings, topicBuffer);
            _topicProducers.Add(topic);            
            return topicBuffer;
        }

        [PublicAPI]
        public IKafkaProducerTopic<TKey, TData> CreateTopicProducer<TKey, TData>([NotNull] string topicName,
           [NotNull] KafkaProducerSettings settings,
           [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
           [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;            
            settings = settings ?? new KafkaProducerSettingsBuilder().Build();
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer);
            var topic = new KafkaProducerTopic(topicName, settings, topicBuffer);
            _topicProducers.Add(topic);
            return topicBuffer;
        }

        [PublicAPI]
        public IKafkaConsumerTopic CreateTopicConsumer([NotNull] string topicName, 
            [NotNull] KafkaConsumerSettings settings)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName)) return null;                        
            settings = settings ?? new KafkaConsumerSettingsBuilder().Build();
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topic = new KafkaConsumerTopic(topicName, settings);
            _topicConsumers.Add(topic);
            return topic;
        }

        [PublicAPI]
        public IKafkaConsumerTopic<TKey,TData> CreateTopicConsumer<TKey, TData>([NotNull] string topicName,
           [NotNull] KafkaConsumerSettings settings,
           [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (serializer == null)) return null;
            settings = settings ?? new KafkaConsumerSettingsBuilder().Build();
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConditionIsAlwaysTrueOrFalse

            var topic = new KafkaConsumerTopic(topicName, settings);
            var wrapper = new KafkaConsumerTopicWrapper<TKey, TData>(topic, serializer);
            _topicConsumers.Add(topic);
            return wrapper;
        }

        [PublicAPI, NotNull]
        public IKafkaClient Build()
        {            
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

            return new KafkaClient(_settings, topics);
        }        
    }
}
