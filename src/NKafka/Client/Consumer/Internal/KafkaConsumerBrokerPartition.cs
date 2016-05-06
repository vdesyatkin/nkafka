using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly KafkaConsumerGroupData Group;        

        public readonly int PartitionId;
        [NotNull] public readonly KafkaConsumerSettings Settings;

        public KafkaConsumerBrokerPartitionStatus Status;
        public bool IsAssigned;
        public bool IsReady => Status == KafkaConsumerBrokerPartitionStatus.Ready && Error == null;
        public bool IsSynchronized => _currentCommitClientOffset <= _currentCommitServerOffset;

        public KafkaConsumerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }

        public DateTime? CommitServerOffsetTimestampUtc { get; private set; }
        public DateTime? ReceiveTimestampUtc { get; private set; }
        public DateTime? ConsumeTimestampUtc { get; private set; }

        public long TotalReceivedMessageCount => _totalReceivedMessageCount;
        public long TotalReceivedMessageSizeBytes => _totalReceivedMessageSizeBytes;
        public int ConsumePendingMessageCount => _consumePendingMessageCount;
        public long ConsumePendingMessageSizeBytes => _consumePendingMessageSizeBytes;
        public long BufferedMessageSizeBytes => _consumePendingMessageSizeBytes + _catchUpPendingMessageSizeBytes;
        public long TotalConsumedMessageCount => _totalConsumedMessageCount;
        public long TotalConsumedMessageSizeBytes => _totalConsumedMessageSizeBytes;        

        private long _currentReceivedClientOffset;
        private long _currentMinAvailableServerOffset;
        private long _currentMaxAvailableServerOffset;
        private long _currentCatchUpGroupServerOffset;
        private long _currentCommitClientOffset;
        private long _currentCommitServerOffset;
        private const long UnknownOffset = -1;

        private long _totalReceivedMessageCount;
        private long _totalReceivedMessageSizeBytes;

        private int _consumePendingMessageCount;
        private long _consumePendingMessageSizeBytes;

        private long _totalConsumedMessageCount;
        private long _totalConsumedMessageSizeBytes;

        private int _catchUpPendingMessageCount;
        private long _catchUpPendingMessageSizeBytes;

        public int? OffsetRequestId;
        
        [NotNull] private readonly ConcurrentQueue<KafkaMessageAndOffset> _consumeMessagesQueue;
        [NotNull] private readonly Queue<KafkaMessageAndOffset> _catchUpMesagesQueue;

        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaConsumerGroupData group,
            [NotNull] KafkaConsumerSettings settings)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            Settings = settings;
            Group = group;          

            _consumeMessagesQueue = new ConcurrentQueue<KafkaMessageAndOffset>();
            _catchUpMesagesQueue = new Queue<KafkaMessageAndOffset>();

            _currentReceivedClientOffset = UnknownOffset;
            _currentMinAvailableServerOffset = UnknownOffset;
            _currentCommitClientOffset = UnknownOffset;
            _currentCommitServerOffset = UnknownOffset;
        }

        public bool CanEnqueueForConsume()
        {
            if (_consumePendingMessageCount + _catchUpPendingMessageCount >= Settings.BufferMaxMessageCount)
            {
                return false;
            }

            if (_consumePendingMessageSizeBytes + _catchUpPendingMessageSizeBytes >= Settings.BufferedMaxSizeBytes)
            {
                return false;
            }

            return true;
        }

        public void EnqueueMessagesForConsume([NotNull, ItemNotNull] IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages.Count == 0) return;

            var lastOffset = _currentReceivedClientOffset;
            var catchUpOffset = _currentCatchUpGroupServerOffset;

            foreach (var message in messages)
            {
                var messageSize = GetMessageSize(message);                

                if (message.Offset <= lastOffset) continue;

                lastOffset = message.Offset;
                Interlocked.Add(ref _totalReceivedMessageSizeBytes, messageSize);
                Interlocked.Increment(ref _totalReceivedMessageCount);

                if (message.Offset > catchUpOffset)
                {                    
                    Interlocked.Add(ref _catchUpPendingMessageSizeBytes, messageSize);
                    Interlocked.Increment(ref _catchUpPendingMessageCount);
                    _catchUpMesagesQueue.Enqueue(message);
                }
                else
                {                    
                    Interlocked.Add(ref _consumePendingMessageSizeBytes, messageSize);
                    Interlocked.Increment(ref _consumePendingMessageCount);
                    _consumeMessagesQueue.Enqueue(message);
                }
            }
            
            _currentReceivedClientOffset = lastOffset;
            ReceiveTimestampUtc = DateTime.UtcNow;
        }

        public bool TryConsumeMessage(out KafkaMessageAndOffset message)
        {
            if (!_consumeMessagesQueue.TryDequeue(out message))
            {
                return false;
            }

            Interlocked.Decrement(ref _consumePendingMessageCount);
            Interlocked.Increment(ref _totalConsumedMessageCount);
            ConsumeTimestampUtc = DateTime.UtcNow;
            if (message == null) return true;

            var messageSize = GetMessageSize(message);            
            Interlocked.Add(ref _consumePendingMessageSizeBytes, -messageSize);
            Interlocked.Add(ref _totalConsumedMessageSizeBytes, messageSize);
            return true;
        }

        private void CatchUpMessages(long catchUpOffset)
        {
            while (_catchUpPendingMessageCount > 0)
            {
                var message = _catchUpMesagesQueue.Peek();
                if (message == null)
                {
                    _catchUpMesagesQueue.Dequeue();
                    continue;
                }

                if (message.Offset > catchUpOffset)
                {
                    break;
                }

                Interlocked.Decrement(ref _catchUpPendingMessageCount);
                Interlocked.Increment(ref _consumePendingMessageCount);                

                var messageSize = GetMessageSize(message);
                Interlocked.Add(ref _catchUpPendingMessageSizeBytes, -messageSize);
                Interlocked.Add(ref _consumePendingMessageSizeBytes, messageSize);

                _consumeMessagesQueue.Enqueue(message);
            }
        }

        #region ReceivedClientOffset

        public long? GetReceivedClientOffset()
        {
            var currenReceivedClientOffset = _currentReceivedClientOffset;
            return currenReceivedClientOffset != UnknownOffset ? currenReceivedClientOffset : (long?) null;
        }

        #endregion ReceivedClientOffset

        #region MinAvailableServerOffset

        public long? GetMinAvailableServerOffset()
        {
            var currenMinAvailableClientOffset = _currentMinAvailableServerOffset;
            return currenMinAvailableClientOffset != UnknownOffset ? currenMinAvailableClientOffset : (long?)null;
        }

        public void SetMinAvailableServerOffset(long offset)
        {
            _currentMinAvailableServerOffset = offset;            
        }

        #endregion MinAvailableServerOffset

        #region MaxAvailableServerOffset

        public long? GetMaxAvailableServerOffset()
        {
            var currenMaxAvailableClientOffset = _currentMaxAvailableServerOffset;
            return currenMaxAvailableClientOffset != UnknownOffset ? currenMaxAvailableClientOffset : (long?)null;
        }

        public void SetMaxAvailableServerOffset(long offset)
        {
            _currentMaxAvailableServerOffset = offset;
        }

        #endregion MaxAvailableServerOffset    

        #region CatchUpGroupServerOffset        

        public void SetCatchUpGroupServerOffset(long? offset)
        {
            _currentCatchUpGroupServerOffset = offset ?? UnknownOffset;
            if (offset != null)
            {
                CatchUpMessages(offset.Value);
            }
        }

        #endregion ReceivedClientOffset

        #region CommitClientOffset

        public long? GetCommitClientOffset()
        {
            var currenConsumedClientOffset = _currentCommitClientOffset;
            return currenConsumedClientOffset != UnknownOffset ? currenConsumedClientOffset : (long?) null;
        }

        public void SetCommitClientOffset(long offset)
        {
            while (offset > _currentCommitClientOffset)
            {
                Interlocked.CompareExchange(ref _currentCommitClientOffset, offset, _currentCommitClientOffset);
            }
        }

        #endregion CommitClientOffset

        #region CommitServerOffset

        public void SetCommitServerOffset(long? offset, DateTime timestampUtc)
        {
            _currentCommitServerOffset = offset ?? UnknownOffset;
            CommitServerOffsetTimestampUtc = timestampUtc;
        }

        #endregion CommitServerOffset

        public void ResetData()
        {
            OffsetRequestId = null;
        }

        public void SetError(KafkaConsumerTopicPartitionErrorCode error)
        {
            ErrorTimestampUtc = DateTime.UtcNow;
            Error = error;
        }

        public void ResetError()
        {
            Error = null;
        }

        [NotNull]
        public KafkaConsumerTopicPartitionOffsetsInfo GetOffsetsInfo()
        {
            var currentReceivedClientOffset = _currentReceivedClientOffset;
            var currentMinAvailableServerOffset = _currentMinAvailableServerOffset;
            var currentMaxAvailableServerOffset = _currentMaxAvailableServerOffset;
            var currentCatchUpGroupServerOffset = _currentCatchUpGroupServerOffset;
            var currentCommitClientOffset = _currentCommitClientOffset;
            var currentCommitServerOffset = _currentCommitServerOffset;

            return new KafkaConsumerTopicPartitionOffsetsInfo(
                currentReceivedClientOffset != UnknownOffset ? currentReceivedClientOffset : (long?)null,
                currentMinAvailableServerOffset != UnknownOffset ? currentMinAvailableServerOffset : (long?)null,
                currentMaxAvailableServerOffset != UnknownOffset ? currentMaxAvailableServerOffset : (long?)null,
                currentCatchUpGroupServerOffset != UnknownOffset ? currentCatchUpGroupServerOffset : (long?)null,
                currentCommitClientOffset != UnknownOffset ? currentCommitClientOffset : (long?)null,
                currentCommitServerOffset != UnknownOffset ? currentCommitServerOffset : (long?)null,
                DateTime.UtcNow);
        }

        public void Unplug()
        {
            ResetData();
            Status = KafkaConsumerBrokerPartitionStatus.NotInitialized;
        }

        public void Clear()
        {
            ResetData();
            ResetError();
            _catchUpMesagesQueue.Clear();
            KafkaMessageAndOffset message;
            while (_consumeMessagesQueue.TryDequeue(out message))
            {
            }
            _consumePendingMessageCount = 0;
            _catchUpPendingMessageCount = 0;
        }

        private static int GetMessageSize([CanBeNull] KafkaMessageAndOffset message)
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