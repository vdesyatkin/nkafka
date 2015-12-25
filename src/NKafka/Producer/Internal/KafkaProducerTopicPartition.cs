using System.Collections.Concurrent;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition : IKafkaProducerMessageQueue
    {
        public readonly string TopicName;

        public readonly int PartitonId;

        [NotNull]
        private readonly ConcurrentQueue<KafkaMessage> _messageQueue;

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;

        public KafkaProducerTopicPartition(string topicName, int partitionId, KafkaBrokerMetadata brokerMetadata)
        {
            TopicName = topicName;
            PartitonId = partitionId;
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            BrokerPartition = new KafkaProducerBrokerPartition(topicName, partitionId, brokerMetadata, this);
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
