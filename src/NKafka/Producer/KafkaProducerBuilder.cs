using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Producer.Internal;

namespace NKafka.Producer
{
    public class KafkaProducerBuilder
    {
        [NotNull]
        private readonly List<KafkaProducerTopic> _topics;
       
        public KafkaProducerBuilder()
        {
            _topics = new List<KafkaProducerTopic>();
        }

        [PublicAPI, CanBeNull]
        public IKafkaProducerTopic AddTopic([NotNull] string topicName, [NotNull] IKafkaProducerPartitioner partitioner)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            if (string.IsNullOrEmpty(topicName) || (partitioner == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            
            var topicBuffer = new KafkaProducerTopicBuffer(partitioner);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);
            _topics.Add(topic);
            return topicBuffer;
        }

        [PublicAPI, CanBeNull]
        public IKafkaProducerTopic<TKey, TData> AddTopic<TKey, TData>([NotNull] string topicName,
           [NotNull] IKafkaProducerPartitioner<TKey, TData> partitioner, 
           [NotNull] IKafkaProducerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            if (string.IsNullOrEmpty(topicName) || (partitioner == null) || (serializer == null)) return null;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse          
            
            var topicBuffer = new KafkaProducerTopicBuffer<TKey, TData>(partitioner, serializer);
            var topic = new KafkaProducerTopic(topicName, topicBuffer);            
            _topics.Add(topic);
            return topicBuffer;
        }

        [PublicAPI, NotNull]
        public IKafkaProducer Build([NotNull]KafkaProducerSettings settings)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settings = settings ?? new KafkaProducerSettingsBuilder(null).Build();

            return new KafkaProducer(settings, _topics);
        }

        [PublicAPI, NotNull]
        public IKafkaProducer Build([NotNull]KafkaProducerSettingsBuilder settingsBuilder)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settingsBuilder = settingsBuilder ?? new KafkaProducerSettingsBuilder(null);

            return Build(settingsBuilder.Build());
        }
    }
}
