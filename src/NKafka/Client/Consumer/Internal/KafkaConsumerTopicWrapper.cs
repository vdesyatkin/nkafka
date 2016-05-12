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
        private readonly IKafkaSerializer<TKey, TData> _serializer;

        public KafkaConsumerTopicWrapper(
            [NotNull] KafkaConsumerTopic topic,
            [NotNull] IKafkaSerializer<TKey, TData> serializer)
        {
            _topic = topic;
            _serializer = serializer;
        }

        public IReadOnlyList<KafkaMessagePackage<TKey, TData>> Consume(int? maxMessageCount = null)
        {
            var packages = _topic.Consume(maxMessageCount);
            var wrappedPackages = new List<KafkaMessagePackage<TKey, TData>>(packages.Count);

            foreach (var package in packages)
            {
                var messages = package.Messages;                

                var genericMessages = new List<KafkaMessage<TKey, TData>>(messages.Count);
                foreach (var message in messages)
                {
                    try
                    {
                        var genericMessage = _serializer.DeserializeMessage(message);
                        if (genericMessage == null) continue;
                        genericMessages.Add(genericMessage);
                    }
                    catch (Exception)
                    {
                        //ignored
                    }
                }

                wrappedPackages.Add(new KafkaMessagePackage<TKey, TData>(package.PackageId, genericMessages));
            }

            return wrappedPackages;
        }

        public bool TryEnqueueCommit(long packageNumber)
        {
            return _topic.TryEnqueueCommit(packageNumber);
        }

        #region Diagnostics

        public bool IsReady => _topic.IsReady;

        public bool IsSynchronized => _topic.IsSynchronized;

        public KafkaConsumerTopicInfo GetDiagnosticsInfo()
        {
            return _topic.GetDiagnosticsInfo();
        }

        #endregion Diagnostics
    }
}
