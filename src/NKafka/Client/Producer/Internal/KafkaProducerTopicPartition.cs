using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    internal sealed class KafkaProducerTopicPartition : IKafkaProducerMessageQueue
    {        
        public readonly int PartitonId;

        public int SendPendingCount => _sendPendingCount;
        public long TotalEnqueuedCount => _totalEnqueuedCount;
        public DateTime? EnqueueTimestampUtc => _enqueueTimestampUtc;

        public long TotalFallbackCount => _totalFallbackCount;
        public DateTime? FallbackTimestampUtc => _fallbackTimestampUtc;

        [NotNull] public readonly KafkaProducerBrokerPartition BrokerPartition;

        [NotNull] private readonly string _topicName;
        [NotNull] private readonly ConcurrentQueue<KafkaMessage> _messageQueue;
        [CanBeNull] private readonly IKafkaProducerFallbackHandler _fallbackHandler;

        private int _sendPendingCount;
        private long _totalEnqueuedCount;
        private DateTime? _enqueueTimestampUtc;
        private long _totalFallbackCount;
        private DateTime? _fallbackTimestampUtc;

        public KafkaProducerTopicPartition([NotNull] string topicName, int partitionId, 
            [NotNull] KafkaProducerSettings settings, [CanBeNull] IKafkaProducerFallbackHandler fallbackHandler)
        {
            _topicName = topicName;
            PartitonId = partitionId;
            _messageQueue = new ConcurrentQueue<KafkaMessage>();
            _fallbackHandler = fallbackHandler;
            BrokerPartition = new KafkaProducerBrokerPartition(partitionId, settings, this);
        }

        public void EnqueueMessage([NotNull] KafkaMessage message)
        {
            _messageQueue.Enqueue(message);
            Interlocked.Increment(ref _sendPendingCount);
            Interlocked.Increment(ref _totalEnqueuedCount);
            _enqueueTimestampUtc = DateTime.UtcNow;
        }

        public bool TryDequeueMessage(out KafkaMessage message)
        {
            if (!_messageQueue.TryDequeue(out message))
            {
                return false;
            }

            Interlocked.Decrement(ref _sendPendingCount);
            return true;
        }        

        public void FallbackMessage(KafkaMessage message, DateTime timestampUtc, KafkaProdcuerFallbackErrorCode reason)
        {
            Interlocked.Increment(ref _totalFallbackCount);
            _fallbackTimestampUtc = timestampUtc;

            try
            {
                var fallbackInfo = new KafkaProducerFallbackInfo(_topicName, PartitonId, timestampUtc, message, reason);
                _fallbackHandler?.HandleMessageFallback(fallbackInfo);
            }
            catch (Exception)
            {
                //ignored
            }
        }
    }
}
