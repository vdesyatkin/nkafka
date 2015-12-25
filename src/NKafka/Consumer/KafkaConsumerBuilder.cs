using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Consumer.Internal;

namespace NKafka.Consumer
{
    public class KafkaConsumerBuilder
    {
        [NotNull]
        private readonly List<KafkaConsumerTopic> _topics;

        public KafkaConsumerBuilder()
        {
            _topics = new List<KafkaConsumerTopic>();
        }

        [PublicAPI]
        public bool TryAddTopic([NotNull] string topicName, [NotNull] IKafkaConsumerTopic dataConsumer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse
            if (string.IsNullOrEmpty(topicName) || dataConsumer == null) return false;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            
            var topic = new KafkaConsumerTopic(topicName, dataConsumer);
            _topics.Add(topic);
            return true;
        }

        [PublicAPI]
        public bool TryAddTopic<TKey, TData>([NotNull] string topicName,
           [NotNull] IKafkaConsumerTopic<TKey, TData> dataConsumer,
           [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer)
        {
            // ReSharper disable ConditionIsAlwaysTrueOrFalse            
            if (string.IsNullOrEmpty(topicName) || (dataConsumer == null) || (serializer == null)) return false;
            // ReSharper restore ConditionIsAlwaysTrueOrFalse
            
            var topic = new KafkaConsumerTopic(topicName, new KafkaConsumerTopicWrapper<TKey, TData>(dataConsumer, serializer));
            _topics.Add(topic);
            return true;
        }

        [PublicAPI, NotNull]
        public IKafkaConsumer Build([NotNull]KafkaConsumerSettings settings)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settings = settings ?? new KafkaConsumerSettingsBuilder(null).Build();

            return new KafkaConsumer(settings, _topics);
        }

        [PublicAPI, NotNull]
        public IKafkaConsumer Build([NotNull]KafkaConsumerSettingsBuilder settingsBuilder)
        {
            // ReSharper disable once ConstantNullCoalescingCondition
            settingsBuilder = settingsBuilder ?? new KafkaConsumerSettingsBuilder(null);

            return Build(settingsBuilder.Build());
        }
    }
}
