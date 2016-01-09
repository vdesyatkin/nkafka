using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition : IKafkaProducerMessageQueue
    {        
        public readonly int PartitonId;        

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;

        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;        

        public KafkaProducerTopicPartition(int partitionId, [NotNull] KafkaProducerSettings settings)
        {            
            PartitonId = partitionId;            
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            BrokerPartition = new KafkaProducerBrokerPartition(partitionId, settings, this);
        }

        public void EnqueueMessage([NotNull] KafkaMessage message)
        {
            _messageQueue.Enqueue(message);
        }

        public bool TryDequeueMessage(out KafkaMessage message)
        {
            return _messageQueue.TryDequeue(out message);
        }
    }
}
