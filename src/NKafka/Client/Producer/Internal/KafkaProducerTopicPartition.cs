using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition : IKafkaProducerMessageQueue
    {        
        public readonly int PartitonId;

        public long EnqueuedCount => _enqueuedCount;
        public DateTime? EnqueueTimestampUtc => _enqueueTimestampUtc;

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;

        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;

        private long _enqueuedCount;
        private DateTime? _enqueueTimestampUtc;

        public KafkaProducerTopicPartition(int partitionId, [NotNull] KafkaProducerSettings settings)
        {            
            PartitonId = partitionId;            
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            BrokerPartition = new KafkaProducerBrokerPartition(partitionId, settings, this);
        }

        public void EnqueueMessage([NotNull] KafkaMessage message)
        {
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _enqueuedCount);
            _enqueueTimestampUtc = DateTime.UtcNow;
        }

        public bool TryDequeueMessage(out KafkaMessage message)
        {
            if (!_messageQueue.TryDequeue(out message))
            {
                return false;
            }

            Interlocked.Decrement(ref _enqueuedCount);
            return true;
        }

        public bool TryPeekMessage(out KafkaMessage message)
        {
            if (!_messageQueue.TryPeek(out message))
            {
                return false;
            }

            return true;
        }
    }
}
