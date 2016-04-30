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
        public KafkaConsumerTopicPartitionErrorCode? Error { get; private set; }
        public DateTime? ErrorTimestampUtc { get; private set; }    

        private long _currentReceivedClientOffset;
        private long _currentConsumedClientOffset;
        private long _currentServerOffset;
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
            _currentConsumedClientOffset = UnknownOffset;
            _currentServerOffset = UnknownOffset;
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
            
            SetReceivedClientOffset(lastMessage.Offset);            

            try
            {
                _messageQueue.Enqueue(messages);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public long? GetReceivedClientOffset()
        {
            var currenEnqueuedClientOffset = _currentReceivedClientOffset;
            return currenEnqueuedClientOffset != UnknownOffset ? currenEnqueuedClientOffset : (long?)null;
        }

        public void SetReceivedClientOffset(long offset)
        {
            while (offset > _currentReceivedClientOffset)
            {
                Interlocked.CompareExchange(ref _currentReceivedClientOffset, offset, _currentReceivedClientOffset);
            }
        }

        public long? GetConsumedClientOffset()
        {
            var currenConsumedClientOffset = _currentConsumedClientOffset;
            return currenConsumedClientOffset != UnknownOffset ? currenConsumedClientOffset : (long?)null;
        }

        public void SetConsumedClientOffset(long offset)
        {
            while (offset > _currentConsumedClientOffset)
            {
                Interlocked.CompareExchange(ref _currentConsumedClientOffset, offset, _currentConsumedClientOffset);
            }
        }        

        public void SetServerOffset(long offset)
        {
            while (offset > _currentServerOffset)
            {
                Interlocked.CompareExchange(ref _currentServerOffset, offset, _currentServerOffset);
            }
        }

        public void ResetData()
        {
            OffsetRequestId = null;
            //todo (E009)
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