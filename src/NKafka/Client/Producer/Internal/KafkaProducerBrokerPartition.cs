﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Producer.Diagnostics;
using NKafka.Client.Producer.Logging;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerPartition
    {
        public readonly int PartitionId;
        [NotNull] public readonly KafkaProducerSettings Settings;
        [NotNull] private readonly string _topicName;
        [CanBeNull] private readonly IKafkaProducerFallbackHandler _fallbackHandler;
        [CanBeNull] public readonly IKafkaProducerTopicLogger Logger;

        public KafkaProducerBrokerPartitionStatus Status;
        public bool IsReady => Status == KafkaProducerBrokerPartitionStatus.Ready && Error == null;
        public bool IsSynchronized => _sendPendingMessageCount == 0;
        public KafkaProducerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }
        [NotNull] public KafkaProducerTopicPartitionLimitInfo LimitInfo { get; private set; }

        private bool _isLocked; // while response pending

        public int RetrySendPendingMessageCount { get; private set; }
        public long TotalSentMessageCount { get; private set; }
        public long TotalSentMessageSizeBytes { get; private set; }
        public DateTime? SendMessageTimestampUtc { get; private set; }

        public int SendPendingMessageCount => _sendPendingMessageCount;
        public long SendPendingMessageSizeBytes => _sendPendingMessageSizeBytes;
        public long TotalEnqueuedMessageCount => _totalEnqueuedMessageCount;
        public long TotalEnqueuedMessageSizeBytes => _totalEnqueuedMessageSizeBytes;
        public DateTime? EnqueueTimestampUtc { get; private set; }

        private int _sendPendingMessageCount;
        private long _sendPendingMessageSizeBytes;
        private long _totalEnqueuedMessageCount;
        private long _totalEnqueuedMessageSizeBytes;

        public long TotalFallbackMessageCount { get; private set; }
        public DateTime? FallbackTimestampUtc { get; private set; }

        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _mainQueue;
        [NotNull] private readonly Queue<KafkaMessage> _retryQueue;

        public KafkaProducerBrokerPartition([NotNull] string topicName, int partitionId,
            [NotNull] KafkaProducerSettings settings,
            [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler,
            [CanBeNull] IKafkaProducerTopicLogger logger)
        {
            PartitionId = partitionId;
            _topicName = topicName;
            Settings = settings;
            _fallbackHandler = fallbackHandler;
            Logger = logger;
            _mainQueue = new ConcurrentQueue<KafkaMessage>();
            _retryQueue = new Queue<KafkaMessage>();
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(settings.MessageMaxSizeByteCount, settings.ProduceRequestMaxSizeByteCount, DateTime.UtcNow);
        }

        public void EnqueueMessage([NotNull] KafkaMessage message)
        {
            _mainQueue.Enqueue(message);
            Interlocked.Increment(ref _sendPendingMessageCount);
            Interlocked.Increment(ref _totalEnqueuedMessageCount);
            var messageSize = GetMessageSize(message);
            Interlocked.Add(ref _sendPendingMessageSizeBytes, messageSize);
            Interlocked.Add(ref _totalEnqueuedMessageSizeBytes, messageSize);
            EnqueueTimestampUtc = DateTime.UtcNow;
        }

        public bool TryLock()
        {
            if (_isLocked) return false;
            _isLocked = true;
            return true;
        }

        public void Unlock()
        {
            _isLocked = false;
        }

        public bool TryPeekMessage(out KafkaMessage message)
        {
            if (_retryQueue.Count == 0)
            {
                return _mainQueue.TryPeek(out message);
            }

            message = _retryQueue.Peek();
            return message != null;
        }

        public void DequeueMessage()
        {
            if (_retryQueue.Count == 0)
            {
                KafkaMessage message;
                _mainQueue.TryDequeue(out message);
                return;
            }

            _retryQueue.Dequeue();
            RetrySendPendingMessageCount = _retryQueue.Count;
        }

        public void RollbackMessages([NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> messages)
        {
            var oldQueue = _retryQueue.ToArray();
            _retryQueue.Clear();

            foreach (var message in messages)
            {
                _retryQueue.Enqueue(message);
            }
            foreach (var message in oldQueue)
            {
                if (message == null) continue;
                _retryQueue.Enqueue(message);
            }

            RetrySendPendingMessageCount = _retryQueue.Count;

            Unlock();
        }

        public void ConfirmMessags([NotNull, ItemNotNull] IReadOnlyList<KafkaMessage> messages)
        {
            TotalSentMessageCount += messages.Count;
            Interlocked.Add(ref _sendPendingMessageCount, -messages.Count);

            long messagesSize = 0;
            foreach (var message in messages)
            {
                messagesSize += GetMessageSize(message);
            }
            Interlocked.Add(ref _sendPendingMessageSizeBytes, -messagesSize);

            TotalSentMessageSizeBytes += messagesSize;
            SendMessageTimestampUtc = DateTime.UtcNow;

            Unlock();
        }

        public void FallbackMessage([NotNull] KafkaMessage message, DateTime timestampUtc, KafkaProducerFallbackErrorCode reason)
        {
            TotalFallbackMessageCount++;
            FallbackTimestampUtc = timestampUtc;

            try
            {
                var fallbackInfo = new KafkaProducerFallbackInfo(_topicName, PartitionId, message, reason);
                _fallbackHandler?.OnMessageFallback(fallbackInfo);
            }
            catch (Exception)
            {
                //ignored
            }
        }

        public void ResetData()
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(Settings.MessageMaxSizeByteCount, Settings.ProduceRequestMaxSizeByteCount, DateTime.UtcNow);
        }

        public void SetMaxMessageSizeByteCount(int maxMessageSizeByteCount)
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(maxMessageSizeByteCount, LimitInfo.MaxBatchSizeByteCount, DateTime.UtcNow);
        }

        public void SetMaxBatchSizeByteCount(int maxMessageCount)
        {
            LimitInfo = new KafkaProducerTopicPartitionLimitInfo(LimitInfo.MaxMessageSizeByteCount, maxMessageCount, DateTime.UtcNow);
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

        public void Unplug()
        {
            ResetData();
            Status = KafkaProducerBrokerPartitionStatus.NotInitialized;
        }

        public void Clear()
        {
            ResetData();
            ResetError();

            KafkaMessage message;
            while (_retryQueue.Count > 0)
            {
                message = _retryQueue.Dequeue();
                if (message == null) continue;
                FallbackMessage(message, DateTime.UtcNow, KafkaProducerFallbackErrorCode.ClientStopped);
            }

            while (_mainQueue.TryDequeue(out message))
            {
                if (message == null) continue;
                FallbackMessage(message, DateTime.UtcNow, KafkaProducerFallbackErrorCode.ClientStopped);
            }

            _sendPendingMessageCount = 0;
            RetrySendPendingMessageCount = 0;
        }

        private static int GetMessageSize([CanBeNull] KafkaMessage message)
        {
            if (message == null) return 0;
            var messageSize = 0;
            if (message.Key != null)
            {
                messageSize += message.Key.Length;
            }
            if (message.Data != null)
            {
                messageSize += message.Data.Length;
            }

            return messageSize;
        }
    }
}