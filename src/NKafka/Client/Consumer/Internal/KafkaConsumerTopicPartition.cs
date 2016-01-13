using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition : IKafkaConsumerMessageQueue
    {
        public readonly int PartitonId;
        
        [NotNull] public readonly KafkaConsumerBrokerPartition BrokerPartition;
        
        public int EnqueuedCount => _enqueuedCount;

        [NotNull] private readonly ConcurrentQueue<KafkaMessageAndOffset> _messageQueue;
        private int _enqueuedCount;
        private long _maxOffset;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaConsumerSettings settings, [NotNull] IKafkaConsumerCoordinator coordinator)
        {            
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, PartitonId, settings, coordinator, this);
            _messageQueue = new ConcurrentQueue<KafkaMessageAndOffset>();
            _maxOffset = -1;
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
            return true;
        }

        public void CommitOffset(long offset)
        {
            BrokerPartition.CommitOffset(offset);
        }
    }
}
