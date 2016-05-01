using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly IKafkaConsumerCoordinator Coordinator;

        public readonly int PartitionId;
        [NotNull] public readonly KafkaConsumerSettings Settings;

        public KafkaConsumerBrokerPartitionStatus Status;
        public bool IsAssigned;
        public bool IsReady => Status == KafkaConsumerBrokerPartitionStatus.Ready && Error == null;
        public KafkaConsumerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }        

        private long _currentReceivedClientOffset;
        private long _currentMinAvailableServerOffset;
        private long _currentMaxAvailableServerOffset;
        private long _currentCommitClientOffset;
        private long _currentCommitServerOffset;
        private const long UnknownOffset = -1;

        public int? OffsetRequestId;

        [NotNull] private readonly IKafkaConsumerMessageQueue _messageQueue;

        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId,
            [NotNull] KafkaConsumerSettings settings,
            [NotNull] IKafkaConsumerCoordinator coordinator, [NotNull] IKafkaConsumerMessageQueue messageQueue)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            Settings = settings;
            Coordinator = coordinator;
            _messageQueue = messageQueue;
            _currentReceivedClientOffset = UnknownOffset;
            _currentMinAvailableServerOffset = UnknownOffset;
            _currentCommitClientOffset = UnknownOffset;
            _currentCommitServerOffset = UnknownOffset;
        }

        public bool CanEnqueue()
        {
            return _messageQueue.CanEnqueue();
        }

        public void EnqueueMessages([NotNull, ItemNotNull] IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages.Count == 0) return;

            var lastMessage = messages[messages.Count - 1];
            if (lastMessage == null) return;

            _currentReceivedClientOffset = lastMessage.Offset;

            var lastOffset = _currentReceivedClientOffset;

            foreach (var message in messages)
            {
                if (message.Offset <= lastOffset) continue;
                lastOffset = message.Offset;
                _messageQueue.EnqueueMessage(message);
            }

            _currentReceivedClientOffset = lastOffset;
        }

        public long? GetReceivedClientOffset()
        {
            var currenReceivedClientOffset = _currentReceivedClientOffset;
            return currenReceivedClientOffset != UnknownOffset ? currenReceivedClientOffset : (long?) null;
        }

        public void SetMinAvailableServerOffset(long offset)
        {
            _currentMinAvailableServerOffset = offset;            
        }

        public void SetMaxAvailableServerOffset(long offset)
        {
            _currentMaxAvailableServerOffset = offset;
        }

        public long? GetMinAvailableServerOffset()
        {
            var currenMinAvailableClientOffset = _currentMinAvailableServerOffset;
            return currenMinAvailableClientOffset != UnknownOffset ? currenMinAvailableClientOffset : (long?) null;
        }

        public long? GetMaxAvailableServerOffset()
        {
            var currenMaxAvailableClientOffset = _currentMaxAvailableServerOffset;
            return currenMaxAvailableClientOffset != UnknownOffset ? currenMaxAvailableClientOffset : (long?)null;
        }

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

        public void SetCommitServerOffset(long? offset)
        {
            _currentCommitServerOffset = offset ?? UnknownOffset;
        }

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
            var currentCommitClientOffset = _currentCommitClientOffset;
            var currentCommitServerOffset = _currentCommitServerOffset;

            return new KafkaConsumerTopicPartitionOffsetsInfo(
                currentReceivedClientOffset != UnknownOffset ? currentReceivedClientOffset : (long?)null,
                currentMinAvailableServerOffset != UnknownOffset ? currentMaxAvailableServerOffset : (long?)null,
                currentMaxAvailableServerOffset != UnknownOffset ? currentMaxAvailableServerOffset : (long?)null,
                currentCommitClientOffset != UnknownOffset ? currentCommitClientOffset : (long?)null,
                currentCommitServerOffset != UnknownOffset ? currentCommitServerOffset : (long?)null,
                DateTime.UtcNow);
        }

        public void Unplug()
        {
            ResetData();
            Status = KafkaConsumerBrokerPartitionStatus.NotInitialized;
        }
    }
}