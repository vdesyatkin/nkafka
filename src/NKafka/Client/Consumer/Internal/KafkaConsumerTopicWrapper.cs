using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;
using NKafka.Client.Consumer.Logging;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicWrapper<TKey, TData> : IKafkaConsumerTopic<TKey, TData>
    {
        string IKafkaConsumerTopic<TKey, TData>.TopicName => _topic.TopicName;
        [NotNull] private readonly KafkaConsumerTopic _topic;

        [CanBeNull] private readonly IKafkaSerializer<TKey, TData> _serializer;

        [CanBeNull] private readonly IKafkaConsumerTopicBufferLogger _logger;

        public KafkaConsumerTopicWrapper(
            [NotNull] KafkaConsumerTopic topic,
            [NotNull] IKafkaSerializer<TKey, TData> serializer,
            [CanBeNull] IKafkaConsumerTopicBufferLogger logger)
        {
            _topic = topic;
            _serializer = serializer;
            _logger = logger;
        }

        public IReadOnlyList<KafkaMessagePackage<TKey, TData>> Consume(int? maxMessageCount = null)
        {
            var packages = _topic.Consume(maxMessageCount);

            var result = new List<KafkaMessagePackage<TKey, TData>>(packages.Count);

            foreach (var package in packages)
            {
                var messages = package.Messages;                

                var genericMessages = new List<KafkaMessage<TKey, TData>>(messages.Count);
                foreach (var message in messages)
                {
                    try
                    {
                        var genericMessage = _serializer?.DeserializeMessage(message);
                        if (genericMessage == null) continue;
                        genericMessages.Add(genericMessage);
                    }
                    catch (Exception exception)
                    {
                        var logger = _logger;
                        if (logger != null)
                        {
                            var errorInfo = new KafkaConsumerTopicSerializationErrorInfo(message, exception);
                            logger.OnSerializationError(errorInfo);
                        }
                    }
                }
                result.Add(new KafkaMessagePackage<TKey, TData>(package.PackageId, package.PartitionId, package.BeginOffset, package.EndOffset, genericMessages));
            }

            return result;
        }

        public void EnqueueCommit(long packageId)
        {
            _topic.EnqueueCommit(packageId);
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
