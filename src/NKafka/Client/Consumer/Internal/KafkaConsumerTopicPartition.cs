using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition : IKafkaConsumerMessageQueue
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitonId;

        [PublicAPI, NotNull]
        public readonly KafkaConsumerBrokerPartition BrokerPartition;

        [PublicAPI]
        public int EnqueuedCount => _enqueuedCount;

        [NotNull] private readonly ConcurrentQueue<KafkaMessageAndOffset> _messageQueue;
        private int _enqueuedCount;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId)
        {
            TopicName = topicName;
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(TopicName, PartitonId, this);
            _messageQueue = new ConcurrentQueue<KafkaMessageAndOffset>();
        }

        public void Enqueue(IReadOnlyList<KafkaMessageAndOffset> messages)
        {
            if (messages == null) return;
            foreach (var message in messages)
            {
                if (message == null) continue;
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
