using System.Collections.Concurrent;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition : IKafkaProducerMessageQueue
    {
        [PublicAPI, NotNull]
        public readonly string TopicName;

        [PublicAPI]
        public readonly int PartitonId;

        [PublicAPI, NotNull]
        public readonly KafkaProducerBrokerPartition BrokerPartition;

        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;        

        public KafkaProducerTopicPartition([NotNull] string topicName, int partitionId)
        {
            TopicName = topicName;
            PartitonId = partitionId;
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            BrokerPartition = new KafkaProducerBrokerPartition(TopicName, PartitonId, this);
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
