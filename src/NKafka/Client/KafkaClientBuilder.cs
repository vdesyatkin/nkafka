using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Internal;
using NKafka.Client.Producer;
using NKafka.Client.Producer.Internal;

namespace NKafka.Client
{
    public class KafkaClientBuilder
    {
        [NotNull, ItemNotNull] private readonly List<KafkaProducerTopic> _topicProducers;

        public KafkaClientBuilder()
        {
            _topicProducers = new List<KafkaProducerTopic>();
        }

        [PublicAPI]
        public IKafkaProducerTopic TryAddTopicProducer([NotNull] string topicName, [NotNull] IKafkaProducerPartitioner partitioner)
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
        public IKafkaProducerTopic<TKey, TData> TryAddTopicProducer<TKey, TData>([NotNull] string topicName,
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

        [PublicAPI, NotNull]
        public IKafkaClient Build([NotNull]KafkaClientSettings settings)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settings = settings ?? new KafkaClientSettingsBuilder(null).Build();

            var topicNames = new HashSet<string>();

            var producers = new Dictionary<string, KafkaProducerTopic>(_topicProducers.Count);
            foreach (var producerPair in producers)
            {
                topicNames.Add(producerPair.Key);
                producers[producerPair.Key] = producerPair.Value;
            }

            var topics = new List<KafkaClientTopic>(topicNames.Count);
            foreach (var topicName in topicNames)
            {
                KafkaProducerTopic producer;
                producers.TryGetValue(topicName, out producer);

                var topic = new KafkaClientTopic(topicName, producer);
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
