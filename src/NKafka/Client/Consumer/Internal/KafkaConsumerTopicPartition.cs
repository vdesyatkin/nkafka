using System;
using System.Threading;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition
    {
        public readonly int PartitonId;
        public bool IsAssigned => BrokerPartition.IsAssigned;        
        
        [NotNull] public readonly KafkaConsumerBrokerPartition BrokerPartition;
        
        public int ConsumePendingCount => BrokerPartition.ConsumePendingMessageCount;
        public long TotalConsumedCount => BrokerPartition.TotalConsumedMessageCount;
        public DateTime? ConsumeTimestampUtc => BrokerPartition.ConsumeTimestampUtc;

        public long TotalReceivedCount => BrokerPartition.TotalReceivedMessageCount;
        public DateTime? ReceiveTimestampUtc => BrokerPartition.ReceiveTimestampUtc;

        public long TotalClientCommitedCount => _totalClientCommitedCount;
        public DateTime? ClientCommitTimestampUtc { get; private set; }

        public DateTime? CommitServerOffsetTimestampUtc => BrokerPartition.CommitServerOffsetTimestampUtc;        
                        
        private long _totalClientCommitedCount;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId,
            [NotNull] KafkaConsumerGroupData group,
            [NotNull] KafkaConsumerSettings settings)
        {            
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, PartitonId, group, settings);            
        }                

        public bool TryConsumeMessage(out KafkaMessageAndOffset message)
        {
            if (!BrokerPartition.TryConsumeMessage(out message))
            {                
                return false;
            }
            
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
