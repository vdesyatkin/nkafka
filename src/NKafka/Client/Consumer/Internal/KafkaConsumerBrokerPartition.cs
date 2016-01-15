using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [NotNull] public readonly string TopicName;
        [NotNull] public readonly IKafkaConsumerCoordinator Coordinator;
        
        public readonly int PartitionId;

        [NotNull] public readonly KafkaConsumerSettings Settings;
        
        public KafkaConsumerBrokerPartitionStatus Status;

        public long CurrentOffset => _lastEnqueuedOffset;

        [NotNull] private readonly IKafkaConsumerMessageQueue _messageQueue;
        
        public int? OffsetRequestId;

        private long _lastEnqueuedOffset;
        private long _lastCommittedOffsetRequired;
        private long _lastCommittedOffsetApproved;

        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaConsumerSettings settings, 
            [NotNull] IKafkaConsumerCoordinator coordinator, [NotNull] IKafkaConsumerMessageQueue messageQueue)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            Settings = settings;
            Coordinator = coordinator;            
            _messageQueue = messageQueue;
            Reset();
        }

        public bool CanEnqueue()
        {
            return _messageQueue.CanEnqueue();
        }

        public void EnqueueMessages(IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages == null) return;
            if (messages.Count == 0) return;

            var newOffset = messages[messages.Count - 1].Offset;

            if (newOffset > _lastEnqueuedOffset)
            {
                _lastEnqueuedOffset = newOffset;
            }

            try
            {
                _messageQueue.Enqueue(messages);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public void RequestCommitOffset(long offset)
        {
            if (offset > _lastCommittedOffsetRequired)
            {
                Interlocked.CompareExchange(ref _lastCommittedOffsetRequired, offset, _lastCommittedOffsetRequired);
            }
        }

        public void ApproveCommitOffset(long offset)
        {
            if (offset > _lastCommittedOffsetApproved)
            {
                Interlocked.CompareExchange(ref _lastCommittedOffsetApproved, offset, _lastCommittedOffsetApproved);
            }
        }

        public long? GetCommitOffset()
        {
            var required = _lastCommittedOffsetRequired;
            var approved = _lastCommittedOffsetApproved;
            return required > approved ? (long?)required : null;
        }

        public void InitOffsets(long initialOffset)
        {
            _lastEnqueuedOffset = initialOffset;
            _lastCommittedOffsetApproved = initialOffset;
            _lastCommittedOffsetRequired = initialOffset;            
        }

        public void Reset()
        {
            OffsetRequestId = null;            
            Status = KafkaConsumerBrokerPartitionStatus.NotInitialized;
            InitOffsets(-1);
        }
    }
}