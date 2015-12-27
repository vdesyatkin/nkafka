using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicWrapper<TKey, TData> : IKafkaConsumerTopic
    {
        [NotNull]
        private readonly IKafkaConsumerTopic<TKey, TData> _dataConsumer;

        [NotNull]
        private readonly IKafkaConsumerSerializer<TKey, TData> _serializer;

        public KafkaConsumerTopicWrapper(
            [NotNull] IKafkaConsumerTopic<TKey, TData> dataConsumer,
            [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer)
        {
            _dataConsumer = dataConsumer;
            _serializer = serializer;
        }

        public void Consume(IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages == null) return;

            var genericMessages = new List<KafkaMessageAndOffset<TKey, TData>>(messages.Count);
            foreach (var message in messages)
            {
                if (message == null) continue;

                try
                {
                    var key = _serializer.DeserializeKey(message.Key);
                    var data = _serializer.DeserializeData(message.Data);
                    var genericMessage = new KafkaMessageAndOffset<TKey, TData>(message.Offset, key, data);
                    genericMessages.Add(genericMessage);
                }
                catch (Exception)
                {
                    //ignored
                }
            }

            try
            {
                _dataConsumer.Consume(genericMessages);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }
}
