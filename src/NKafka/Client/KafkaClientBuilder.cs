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
            [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler = null,
            [CanBeNull] KafkaProducerSettings settings = null)
        {            
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;            
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer(partitioner, fallbackHandler);
            var topic = new KafkaProducerTopic(topicName, settings ?? KafkaProducerSettingsBuilder.Default, topicBuffer);
            _topicProducers.Add(topic);
            return new KafkaProducerTopicFacade(topicName, topicBuffer, topic);
        }
        
        public IKafkaProducerTopic<TKey, TData> CreateTopicProducer<TKey, TData>([NotNull] string topicName,           
           [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner,
           [NotNull] IKafkaProducerSerializer<TKey, TData> serializer,
           [CanBeNull] IKafkaProducerFallbackHandler<TKey, TData> fallbackHandler = null,
           [CanBeNull] KafkaProducerSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;            
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer, fallbackHandler);
            var topic = new KafkaProducerTopic(topicName, settings ?? KafkaProducerSettingsBuilder.Default, topicBuffer);
            _topicProducers.Add(topic);
            return new KafkaProducerTopicFacade<TKey, TData>(topicName, topicBuffer, topic);
        }
        
        public IKafkaConsumerTopic CreateTopicConsumer([NotNull] string topicName, [NotNull] IKafkaConsumerGroup group,
            [CanBeNull] KafkaConsumerSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable ConstantNullCoalescingCondition
            // ReSharper disable ConstantConditionalAccessQualifier
            if (string.IsNullOrEmpty(topicName)) return null;            
            if (string.IsNullOrEmpty(group?.GroupName)) return null;            
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition            

            var topic = new KafkaConsumerTopic(topicName, 
                group.GroupName, null, 
                settings ?? KafkaConsumerSettingsBuilder.Default);
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
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topic = new KafkaConsumerTopic(topicName, 
                group.GroupName, null, 
                settings ?? KafkaConsumerSettingsBuilder.Default);
            var wrapper = new KafkaConsumerTopicWrapper<TKey, TData>(topic, serializer);
            _topicConsumers.Add(topic);
            return wrapper;
        }

        public IKafkaConsumerTopic CreateTopicCatchUpConsumer([NotNull] string topicName, 
            [NotNull] IKafkaConsumerGroup consumerGroup,
            [NotNull] IKafkaConsumerGroup catchUpGroup,
            [CanBeNull] KafkaConsumerSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            // ReSharper disable ConstantNullCoalescingCondition
            // ReSharper disable ConstantConditionalAccessQualifier
            if (string.IsNullOrEmpty(topicName)) return null;
            if (string.IsNullOrEmpty(consumerGroup?.GroupName)) return null;
            if (string.IsNullOrEmpty(catchUpGroup?.GroupName)) return null;
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition            

            var topic = new KafkaConsumerTopic(topicName, 
                consumerGroup.GroupName, catchUpGroup.GroupName,
                settings ?? KafkaConsumerSettingsBuilder.Default);
            _topicConsumers.Add(topic);
            return topic;
        }

        public IKafkaConsumerTopic<TKey, TData> CreateTopicCatchUpConsumer<TKey, TData>([NotNull] string topicName, 
            [NotNull] IKafkaConsumerGroup consumerGroup,
            [NotNull] IKafkaConsumerGroup catchUpGroup,
            [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer,
            [CanBeNull] KafkaConsumerSettings settings = null
           )
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            // ReSharper disable ConstantConditionalAccessQualifier
            if (string.IsNullOrEmpty(topicName) || (serializer == null)) return null;
            if (string.IsNullOrEmpty(consumerGroup?.GroupName)) return null;
            if (string.IsNullOrEmpty(catchUpGroup?.GroupName)) return null;
            // ReSharper restore ConstantConditionalAccessQualifier
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var topic = new KafkaConsumerTopic(topicName,
                consumerGroup.GroupName, catchUpGroup.GroupName,
                settings ?? KafkaConsumerSettingsBuilder.Default);
            var wrapper = new KafkaConsumerTopicWrapper<TKey, TData>(topic, serializer);
            _topicConsumers.Add(topic);
            return wrapper;
        }

        public IKafkaConsumerGroup CreateConsumerGroup([NotNull] string groupName, KafkaConsumerGroupType groupType, [CanBeNull] KafkaConsumerGroupSettings settings = null)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            // ReSharper disable ConstantNullCoalescingCondition
            if (string.IsNullOrEmpty(groupName)) return null;
            settings = settings ?? KafkaConsumerGroupSettingsBuilder.Default;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            // ReSharper restore ConstantNullCoalescingCondition

            var group = new KafkaConsumerGroup(groupName, groupType, settings);
            _consumerGroups[groupName] = group;

            return group;
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
                topicNames.Add(consumer.TopicName);
                consumers[consumer.TopicName] = consumer;
            }

            // create topics and groupping by consumer group
            var topics = new List<KafkaClientTopic>(topicNames.Count);            
            var groupTopicsDictionary = new Dictionary<string, List<KafkaClientTopic>>(_topicConsumers.Count);
            foreach (var topicName in topicNames)
            {
                if (topicName == null) continue;

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
                    if (!groupTopicsDictionary.TryGetValue(groupName, out groupTopicList) || groupTopicList == null)
                    {
                        groupTopicList = new List<KafkaClientTopic>();
                        groupTopicsDictionary[groupName] = groupTopicList;
                    }
                    groupTopicList.Add(topic);
                }

                var catchUpGroupName = consumer?.CatchUpGroupName;
                if (!string.IsNullOrEmpty(catchUpGroupName))
                {
                    if (!_consumerGroups.ContainsKey(catchUpGroupName)) continue;

                    List<KafkaClientTopic> groupTopicList;
                    if (!groupTopicsDictionary.TryGetValue(catchUpGroupName, out groupTopicList) || groupTopicList == null)
                    {
                        groupTopicList = new List<KafkaClientTopic>();
                        groupTopicsDictionary[catchUpGroupName] = groupTopicList;
                    }
                    groupTopicList.Add(topic);
                }
            }

            // create groups
            var groups = new List<KafkaClientGroup>(groupTopicsDictionary.Count);
            var groupsDictionary = new Dictionary<string, KafkaClientGroup>(groupTopicsDictionary.Count);
            foreach (var groupPair in groupTopicsDictionary)
            {
                var groupName = groupPair.Key;
                var groupTopics = groupPair.Value;

                if (groupName == null || groupTopics == null) continue;

                KafkaConsumerGroup group;
                if (!_consumerGroups.TryGetValue(groupName, out group) || group == null)
                {
                    continue;
                }

                var clientGroup = new KafkaClientGroup(groupName, group.GroupType, groupTopics, group.Settings);
                group.ClientGroup = clientGroup;                

                groups.Add(clientGroup);
                groupsDictionary[groupName] = clientGroup;
            }

            // apply group for topics
            foreach (var topic in topics)
            {
                if (topic == null) continue;

                KafkaConsumerTopic consumer;
                if (!consumers.TryGetValue(topic.TopicName, out consumer) || consumer == null)
                {
                    continue;
                }

                KafkaClientGroup consumerGroup;
                if (!groupsDictionary.TryGetValue(consumer.GroupName, out consumerGroup) || consumerGroup == null)
                {
                    continue;
                }

                KafkaClientGroup catchUpGroup;
                if (string.IsNullOrEmpty(consumer.CatchUpGroupName) || !groupsDictionary.TryGetValue(consumer.CatchUpGroupName, out catchUpGroup))
                {
                    catchUpGroup = null;
                }

                topic.Consumer?.ApplyCoordinator(consumerGroup.Coordinator, catchUpGroup?.Coordinator);
            }

            return new KafkaClient(_settings, topics, groups);
        }
    }
}
