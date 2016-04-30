using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicWrapper<TKey, TData> : IKafkaConsumerTopic<TKey, TData>
    {
        [NotNull]
        private readonly KafkaConsumerTopic _topic;

        [NotNull]
        private readonly IKafkaConsumerSerializer<TKey, TData> _serializer;

        public KafkaConsumerTopicWrapper(
            [NotNull] KafkaConsumerTopic topic,
            [NotNull] IKafkaConsumerSerializer<TKey, TData> serializer)
        {
            _topic = topic;
            _serializer = serializer;
        }

        public KafkaMessagePackage<TKey, TData> Consume(int? maxMessageCount = null)
        {
            var package = _topic.Consume(maxMessageCount);
            var messages = package?.Messages;
            if (messages == null) return null;

            var genericMessages = new List<KafkaMessage<TKey, TData>>(messages.Count);
            foreach (var message in messages)
            {                
                try
                {
                    var key = _serializer.DeserializeKey(message.Key);
                    var data = _serializer.DeserializeData(message.Data);
                    var genericMessage = new KafkaMessage<TKey, TData>(key, data);
                    genericMessages.Add(genericMessage);
                }
                catch (Exception)
                {
                    //ignored
                }
            }

            return new KafkaMessagePackage<TKey, TData>(package.PackageNumber, genericMessages);
        }

        public void EnqueueCommit(long packageNumber)
        {
            _topic.EnqueueCommit(packageNumber);
        }

        public KafkaConsumerTopicInfo GetDiagnosticsInfo()
        {
            return _topic.GetDiagnosticsInfo();
        }
    }
}
