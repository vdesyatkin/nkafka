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
