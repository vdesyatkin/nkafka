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
        public bool IsReady => Status == KafkaConsumerBrokerPartitionStatus.Ready && Error == null;
        public KafkaConsumerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }    

        private long _currentReceivedClientOffset;
        private long _currentAvailableServerOffset;
        private long _currentCommitClientOffset;
        private long _currentCommitServerOffset;
        private const long UnknownOffset = -1;

        public int? OffsetRequestId;

        [NotNull]
        private readonly IKafkaConsumerMessageQueue _messageQueue;


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
            _currentAvailableServerOffset = UnknownOffset;
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
            var currenEnqueuedClientOffset = _currentReceivedClientOffset;
            return currenEnqueuedClientOffset != UnknownOffset ? currenEnqueuedClientOffset : (long?)null;
        }

        public void SetAvailableServerOffset(long offset)
        {
            while (offset > _currentAvailableServerOffset)
            {
                Interlocked.CompareExchange(ref _currentAvailableServerOffset, offset, _currentAvailableServerOffset);
            }
        }

        public long? GetAvailableServerOffset()
        {
            var currenEnqueuedClientOffset = _currentReceivedClientOffset;
            return currenEnqueuedClientOffset != UnknownOffset ? currenEnqueuedClientOffset : (long?)null;
        }

        public long? GetCommitClientOffset()
        {
            var currenConsumedClientOffset = _currentCommitClientOffset;
            return currenConsumedClientOffset != UnknownOffset ? currenConsumedClientOffset : (long?)null;
        }

        public void SetCommitClientOffset(long offset)
        {
            while (offset > _currentCommitClientOffset)
            {
                Interlocked.CompareExchange(ref _currentCommitClientOffset, offset, _currentCommitClientOffset);
            }
        }        

        public void SetCommitServerOffset(long offset)
        {
            while (offset > _currentCommitServerOffset)
            {
                Interlocked.CompareExchange(ref _currentCommitServerOffset, offset, _currentCommitServerOffset);
            }
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
    }
}