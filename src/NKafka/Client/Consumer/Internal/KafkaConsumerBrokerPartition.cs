using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerBrokerPartition
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitionId;

        [PublicAPI, NotNull]
        public readonly KafkaConsumerSettings Settings;

        [PublicAPI]
        public KafkaConsumerBrokerPartitionStatus Status;

        public long CurrentOffset => _lastEnqueuedOffset;

        [NotNull]
        private readonly IKafkaConsumerMessageQueue _messageQueue;
        
        public int? OffsetRequestId;

        private long _lastEnqueuedOffset;
        private long _lastCommittedOffsetRequired;

        public KafkaConsumerBrokerPartition([NotNull] string topicName, int partitionId, [NotNull] KafkaConsumerSettings settings, [NotNull] IKafkaConsumerMessageQueue messageQueue)
        {
            TopicName = topicName;
            Settings = settings;
            PartitionId = partitionId;
            _messageQueue = messageQueue;
            Reset();
        }

        public void EnqueueMessages(IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages == null) return;
            if (messages.Count == 0) return;

            var newOffset = messages[messages.Count - 1].Offset;

            if (newOffset > _lastEnqueuedOffset)
            {
                _lastCommittedOffsetRequired = newOffset;
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

        public void CommitOffset(long offset)
        {
            if (offset > _lastCommittedOffsetRequired)
            {
                Interlocked.CompareExchange(ref _lastCommittedOffsetRequired, offset, _lastCommittedOffsetRequired);
            }
        }

        public void InitOffsets(long initialOffset)
        {
            _lastEnqueuedOffset = initialOffset;
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
