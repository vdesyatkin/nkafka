using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition : IKafkaConsumerMessageQueue
    {
        public readonly int PartitonId;

        [NotNull] private readonly KafkaConsumerSettings _settings;
        
        [NotNull] public readonly KafkaConsumerBrokerPartition BrokerPartition;
        
        public int EnqueuedCount => _enqueuedCount;        

        [NotNull] private readonly ConcurrentQueue<KafkaMessageAndOffset> _messageQueue;
        private int _enqueuedCount;
        private int _enqueuedSize;
        private long _maxOffset;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaConsumerSettings settings, [NotNull] IKafkaConsumerCoordinator coordinator)
        {            
            PartitonId = partitionId;
            _settings = settings;
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, PartitonId, settings, coordinator, this);
            _messageQueue = new ConcurrentQueue<KafkaMessageAndOffset>();
            _maxOffset = -1;
        }

        public bool CanEnqueue()
        {
            var settingsBytesLimit = _settings.BufferedMaxSizeBytes;
            if (settingsBytesLimit.HasValue)
            {
                if (settingsBytesLimit.Value <= _enqueuedSize)
                {
                    return false;
                }
            }

            var settingsCountLimit = _settings.BufferMaxMessageCount;
            if (settingsCountLimit.HasValue)
            {
                if (settingsCountLimit <= _enqueuedCount)
                {
                    return false;
                }
            }

            return true;
        }

        public void Enqueue(IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages == null) return;
            foreach (var message in messages)
            {
                if (message == null) continue;
                if (message.Offset <= _maxOffset) continue;
                _maxOffset = message.Offset;
                Interlocked.Increment(ref _enqueuedCount);
                var messageSize = 0;
                if (message.Key != null)
                {
                    messageSize += message.Key.Length;
                }
                if (message.Data != null)
                {
                    messageSize += message.Data.Length;
                }
                Interlocked.Add(ref _enqueuedSize, messageSize);
                _messageQueue.Enqueue(message);
            }
        }

        public bool TryDequeue(out KafkaMessageAndOffset message)
        {
            if (!_messageQueue.TryDequeue(out message))
            {                
                return false;
            }

            Interlocked.Decrement(ref _enqueuedCount);
            if (message == null) return true;

            var messageSize = 0;
            if (message.Key != null)
            {
                messageSize += message.Key.Length;
            }
            if (message.Data != null)
            {
                messageSize += message.Data.Length;
            }
            Interlocked.Add(ref _enqueuedSize, -messageSize);
            return true;
        }

        public void RequestCommitOffset(long offset)
        {
            BrokerPartition.RequestCommitOffset(offset);
        }

        public void ApproveCommitOffset(long offset)
        {        
            BrokerPartition.ApproveCommitOffset(offset);    
        }

        public long? GetCommitOffset()
        {
            return BrokerPartition.GetCommitOffset();
        }
    }
}
