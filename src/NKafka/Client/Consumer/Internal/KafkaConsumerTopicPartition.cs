using System;
using System.Collections.Concurrent;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition : IKafkaConsumerMessageQueue
    {
        public readonly int PartitonId;
        public bool IsAssigned => BrokerPartition.IsAssigned;

        [NotNull] private readonly KafkaConsumerSettings _settings;
        
        [NotNull] public readonly KafkaConsumerBrokerPartition BrokerPartition;
        
        public int ConsumePendingCount => _consumePendingCount;
        public long TotalConsumedCount => _totalConsumedCount;
        public DateTime? ConsumeTimestampUtc { get; private set; }

        public long TotalReceivedCount => _totalReceivedCount;
        public DateTime? ReceiveTimestampUtc { get; private set; }        

        public long TotalClientCommitedCount => _totalClientCommitedCount;
        public DateTime? ClientCommitTimestampUtc { get; private set; }

        public DateTime? ServerCommitTimestampUtc => BrokerPartition.ServerCommitTimestampUtc;

        [NotNull] private readonly ConcurrentQueue<KafkaMessageAndOffset> _messageQueue;

        private int _consumePendingCount;
        private long _totalReceivedCount;
        private long _totalConsumedCount;
        private long _totalClientCommitedCount;

        private long _enqueuedSize;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId,
            [NotNull] KafkaConsumerGroupData group,
            [NotNull] KafkaConsumerSettings settings)
        {            
            PartitonId = partitionId;
            _settings = settings;
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, PartitonId, group, settings, this);
            _messageQueue = new ConcurrentQueue<KafkaMessageAndOffset>();            
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
                if (settingsCountLimit <= _consumePendingCount)
                {
                    return false;
                }
            }

            return true;
        }

        public void EnqueueMessage(KafkaMessageAndOffset message)
        {
            if (message == null) return;
            
            Interlocked.Increment(ref _consumePendingCount);
            Interlocked.Increment(ref _totalReceivedCount);
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

            ReceiveTimestampUtc = DateTime.UtcNow;
        }

        public bool TryDequeue(out KafkaMessageAndOffset message)
        {
            if (!_messageQueue.TryDequeue(out message))
            {                
                return false;
            }

            Interlocked.Increment(ref _totalConsumedCount);
            Interlocked.Decrement(ref _consumePendingCount);
            ConsumeTimestampUtc = DateTime.UtcNow;
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

        public void SetCommitClientOffset(long beginOffset, long endOffset)
        {
            BrokerPartition.SetCommitClientOffset(endOffset);
            Interlocked.Add(ref _totalClientCommitedCount, endOffset - beginOffset);
            ClientCommitTimestampUtc = DateTime.UtcNow;
        }
                
        public long? GetCommitClientOffset()
        {
            return BrokerPartition.GetCommitClientOffset();
        }
    }
}
