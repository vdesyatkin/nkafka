using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerBrokerPartition
    {        
        public readonly int PartitionId;

        [NotNull] public readonly KafkaProducerSettings Settings;

        [NotNull] private readonly IKafkaProducerMessageQueue _mainQueue;
        [NotNull] private readonly Queue<KafkaMessage> _retryQueue;

        public bool NeedRearrange;

        public KafkaProducerBrokerPartition(int partitionId, [NotNull] KafkaProducerSettings settings, [NotNull] IKafkaProducerMessageQueue mainQueue)
        {     
            PartitionId = partitionId;
            Settings = settings;
            _mainQueue = mainQueue;
            _retryQueue = new Queue<KafkaMessage>();
        }

        public bool TryDequeueMessage(out KafkaMessage message)
        {
            if (_retryQueue.Count == 0)
            {
                return _mainQueue.TryDequeueMessage(out message);
            }

            message = _retryQueue.Dequeue();
            return message != null;
        }

        public void RollbackMessags(IReadOnlyList<KafkaMessage> messages)
        {
            if (messages == null) return;

            var oldQueue = _retryQueue.ToArray();
            _retryQueue.Clear();

            foreach (var message in messages)
            {
                if (message == null) continue;
                _retryQueue.Enqueue(message);
            }
            foreach (var message in oldQueue)
            {
                if (message == null) continue;
                _retryQueue.Enqueue(message);
            }
        }
    }
}
