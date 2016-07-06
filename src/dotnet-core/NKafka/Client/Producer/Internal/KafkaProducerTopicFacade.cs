using System;
using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicFacade : IKafkaProducerTopic
    {        
        public string TopicName { get; }        

        [NotNull] private readonly KafkaProducerTopicBuffer _buffer;
        [NotNull] private readonly KafkaProducerTopic _topic;

        public KafkaProducerTopicFacade([NotNull] string topicName, [NotNull] KafkaProducerTopicBuffer buffer, [NotNull] KafkaProducerTopic topic)
        {
            TopicName = topicName;
            _buffer = buffer;
            _topic = topic;
        }

        public void EnqueueMessage([CanBeNull] byte[] key, [CanBeNull] byte[] data, DateTime? timestampUtc = null)
        {
            _buffer.EnqueueMessage(new KafkaMessage(key, data, timestampUtc));
        }

        public void EnqueueMessage([CanBeNull]  byte[] data, DateTime? timestampUtc = null)
        {
            _buffer.EnqueueMessage(new KafkaMessage(null, data, timestampUtc));
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage message)
        {
            _buffer.EnqueueMessage(message);
        }

        #region Diagnostics

        public bool IsReady => _topic.IsReady;

        public bool IsSynchronized => _topic.IsSynchronized;
        
        public KafkaProducerTopicInfo GetDiagnosticsInfo()
        {
            return _topic.GetDiagnosticsInfo();
        }

        #endregion Diagnostics
    }

    internal sealed class KafkaProducerTopicFacade<TKey, TData> : IKafkaProducerTopic<TKey, TData>
    {        
        public string TopicName { get; }        

        [NotNull] private readonly KafkaProducerTopicBuffer<TKey, TData> _buffer;
        [NotNull] private readonly KafkaProducerTopic _topic;

        public KafkaProducerTopicFacade([NotNull] string topicName, [NotNull] KafkaProducerTopicBuffer<TKey, TData> buffer, [NotNull] KafkaProducerTopic topic)
        {
            TopicName = topicName;
            _buffer = buffer;
            _topic = topic;
        }

        public void EnqueueMessage([CanBeNull] TKey key, [CanBeNull] TData data, DateTime? timestampUtc = null)
        {
            _buffer.EnqueueMessage(new KafkaMessage<TKey, TData>(key, data, timestampUtc));
        }

        public void EnqueueMessage([CanBeNull]  TData data, DateTime? timestampUtc = null)
        {
            _buffer.EnqueueMessage(new KafkaMessage<TKey, TData>(default(TKey), data, timestampUtc));
        }

        public void EnqueueMessage([CanBeNull] KafkaMessage<TKey, TData> message)
        {
            _buffer.EnqueueMessage(message);
        }

        #region Diagnostics

        public bool IsReady => _topic.IsReady;

        public bool IsSynchronized => _topic.IsSynchronized;

        public KafkaProducerTopicInfo GetDiagnosticsInfo()
        {
            return _topic.GetDiagnosticsInfo();
        }

        #endregion Diagnostics
    }
}
