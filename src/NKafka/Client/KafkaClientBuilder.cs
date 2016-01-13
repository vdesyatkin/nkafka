using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer;
using NKafka.Client.Consumer.Internal;
using NKafka.Client.ConsumerGroup;
using NKafka.Client.Internal;
using NKafka.Client.Producer;
using NKafka.Client.Producer.Internal;

namespace NKafka.Client
{
    [PublicAPI]
    public class KafkaClientBuilder
    {
        [NotNull, ItemNotNull] private readonly List<KafkaProducerTopic> _topicProducers;
        [NotNull, ItemNotNull] private readonly List<KafkaConsumerTopic> _topicConsumers;
        [NotNull] private readonly Dictionary<string, KafkaConsumerGroup> _consumerGroups;
        [NotNull] private readonly KafkaClientSettings _settings;

        public KafkaClientBuilder([NotNull] KafkaBrokerInfo metadataBroker)
            : this(new KafkaClientSettingsBuilder(metadataBroker).Build())
        {            
        }

        public KafkaClientBuilder([NotNull]KafkaClientSettings settings)
        {
            _topicProducers = new List<KafkaProducerTopic>();
            _topicConsumers = new List<KafkaConsumerTopic>();
            _consumerGroups = new Dictionary<string, KafkaConsumerGroup>();
            // ReSharper disable once ConstantNullCoalescingCondition
            _settings = settings ?? new KafkaClientSettingsBuilder(null).Build();
        }
        
        public IKafkaProducerTopic CreateTopicProducer([NotNull] string topicName,             
            [NotNull] IKafkaProducerPartitioner partitioner,
            [CanBeNull] KafkaProducerSettings settings = null)
        {            
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;
            settings = settings ?? KafkaProducerSettingsBuilder.Default;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer(partitioner);
            var topic = new KafkaProducerTopic(topicName, settings, topicBuffer);
            _topicProducers.Add(topic);            
            return topicBuffer;
        }
        
        public IKafkaProducerTopic<TKey, TData> CreateTopicProducer<TKey, TData>([NotNull] string topicName,           
           [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
           [NotNull] IKafkaProducerSerializer<TKey, TData> serializer,
           [CanBeNull] KafkaProducerSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;            
            settings = settings ?? KafkaProducerSettingsBuilder.Default;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer);
            var topic = new KafkaProducerTopic(topicName, settings, topicBuffer);
            _topicProducers.Add(topic);
            return topicBuffer;
        }
        
        public IKafkaConsumerTopic CreateTopicConsumer([NotNull] string topicName, [NotNull] IKafkaConsumerGroup group,
            [CanBeNull] KafkaConsumerSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable ConstantNullCoalescingCondition
            // ReSharper disable ConstantConditionalAccessQualifier
            if (string.IsNullOrEmpty(topicName)) return null;            
            if (string.IsNullOrEmpty(group?.GroupName)) return null;
            settings = settings ?? KafkaConsumerSettingsBuilder.Default;
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition            

            var topic = new KafkaConsumerTopic(topicName, group.GroupName, settings);
            _topicConsumers.Add(topic);
            return topic;
        }
        
        public IKafkaConsumerTopic<TKey,TData> CreateTopicConsumer<TKey, TData>([NotNull] string topicName, [NotNull] IKafkaConsumerGroup group,
            [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer,
            [CanBeNull] KafkaConsumerSettings settings = null
           )
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            // ReSharper disable ConstantConditionalAccessQualifier
            if (string.IsNullOrEmpty(topicName) || (serializer == null)) return null;
            if (string.IsNullOrEmpty(group?.GroupName)) return null;
            settings = settings ?? KafkaConsumerSettingsBuilder.Default;
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topic = new KafkaConsumerTopic(topicName, group.GroupName, settings);
            var wrapper = new KafkaConsumerTopicWrapper<TKey, TData>(topic, serializer);
            _topicConsumers.Add(topic);
            return wrapper;
        }

        public IKafkaConsumerGroup CreateConsumerGroup([NotNull] string groupName, [CanBeNull] KafkaConsumerGroupSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(groupName)) return null;
            settings = settings ?? KafkaConsumerGroupSettingsBuilder.Default;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            _consumerGroups[groupName] = new KafkaConsumerGroup(groupName, settings);

            return new KafkaConsumerGroup(groupName, settings);
        }

        [NotNull]
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
                if (!topicNames.Add(consumer.TopicName)) continue;

                consumers[consumer.TopicName] = consumer;
            }

            var topics = new List<KafkaClientTopic>(topicNames.Count);            
            var groupTopicsDictionary = new Dictionary<string, List<KafkaClientTopic>>(_topicConsumers.Count);
            foreach (var topicName in topicNames)
            {
                KafkaProducerTopic producer;
                producers.TryGetValue(topicName, out producer);

                KafkaConsumerTopic consumer;
                consumers.TryGetValue(topicName, out consumer);

                var topic = new KafkaClientTopic(topicName, producer, consumer);
                topics.Add(topic);

                var groupName = consumer?.GroupName;
                if (!string.IsNullOrEmpty(groupName))
                {            
                    if (!_consumerGroups.ContainsKey(groupName)) continue;

                    List<KafkaClientTopic> groupTopicList;
                    if (!groupTopicsDictionary.TryGetValue(groupName, out groupTopicList))
                    {
                        groupTopicList = new List<KafkaClientTopic>();
                        groupTopicsDictionary[groupName] = groupTopicList;
                    }
                    groupTopicList.Add(topic);
                }
            }

            var groups = new List<KafkaClientGroup>(groupTopicsDictionary.Count);
            foreach (var groupPair in groupTopicsDictionary)
            {
                var groupName = groupPair.Key;
                var groupTopics = groupPair.Value;

                KafkaConsumerGroup group;
                if (!_consumerGroups.TryGetValue(groupName, out group))
                {
                    continue;
                }

                var clientGroup = new KafkaClientGroup(groupName, groupTopics, group.Settings);

                foreach (var topic in groupTopics)
                {
                    topic.ApplyConsumerCoordinator(clientGroup);
                }

                groups.Add(clientGroup);
            }

            return new KafkaClient(_settings, topics, groups);
        }
    }
}
