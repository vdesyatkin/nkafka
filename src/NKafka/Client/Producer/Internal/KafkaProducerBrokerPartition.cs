using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerPartition
    {        
        public readonly int PartitionId;
        [NotNull] public readonly KafkaProducerSettings Settings;

        public KafkaProducerBrokerPartitionStatus Status;
        public KafkaProducerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }
        [NotNull] public KafkaProducerTopicPartitionLimitInfo LimitInfo { get; private set; }

        public long RetryEnqueuedMessageCount { get; private set; }
        public long SentMessageCount { get; private set; }
        public DateTime? SendMessageTimestampUtc { get; private set; }

        [NotNull] private readonly IKafkaProducerMessageQueue _mainQueue;
        [NotNull] private readonly Queue<KafkaMessage> _retryQueue;        

        public KafkaProducerBrokerPartition(int partitionId, [NotNull] KafkaProducerSettings settings, [NotNull] IKafkaProducerMessageQueue mainQueue)
        {     
            PartitionId = partitionId;
            Settings = settings;
            _mainQueue = mainQueue;
            _retryQueue = new Queue<KafkaMessage>();
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(DateTime.UtcNow, settings.MaxMessageSizeByteCount, settings.BatchMaxMessageCount);
        }        

        public bool TryDequeueMessage(out KafkaMessage message)
        {
            if (_retryQueue.Count == 0)
            {
                return _mainQueue.TryDequeueMessage(out message);
            }

            message = _retryQueue.Dequeue();
            RetryEnqueuedMessageCount = _retryQueue.Count;
            return message != null;
        }

        public void RollbackMessags(IReadOnlyList<KafkaMessage> messages)
        {            
            if (messages == null) return;

            var oldQueue = _retryQueue.ToArray();
            _retryQueue.Clear();

            foreach (var message in messages)
            {
                if (message == null) continue;
                _retryQueue.Enqueue(message);
            }
            foreach (var message in oldQueue)
            {
                if (message == null) continue;
                _retryQueue.Enqueue(message);
            }

            RetryEnqueuedMessageCount = _retryQueue.Count;            
        }

        public void ConfirmMessags(IReadOnlyList<KafkaMessage> messages)
        {
            if (messages == null) return;

            SentMessageCount += messages.Count;
            SendMessageTimestampUtc = DateTime.UtcNow;            
        }

        public void FallbackMessage([NotNull] KafkaMessage message, DateTime timestampUtc, KafkaProdcuerFallbackErrorCode reason)
        {
            try
            {
                _mainQueue.FallbackMessage(message, timestampUtc, reason);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void ResetLimits()
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(DateTime.UtcNow, Settings.MaxMessageSizeByteCount, Settings.BatchMaxMessageCount);
        }

        public void SetMaxMessageSizeByteCount(int maxMessageSizeByteCount)
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(DateTime.UtcNow, maxMessageSizeByteCount, LimitInfo.MaxMessageCount);
        }

        public void SetMaxMessageCount(int maxMessageCount)
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(DateTime.UtcNow, LimitInfo.MaxMessageSizeByteCount, maxMessageCount);
        }

        public void SetError(KafkaProducerTopicPartitionErrorCode error)
        {
            ErrorTimestampUtc = DateTime.UtcNow;
            Error = error;
        }

        public void ResetError()
        {            
            Error = null;
        }
    }
}
