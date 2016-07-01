using System;
using System.Threading;
using JetBrains.Annotations;
using NKafka.Client.Consumer.Logging;

namespace NKafka.Client.Consumer.Internal
{
    internal sealed class KafkaConsumerTopicPartition
    {
        public readonly int PartitonId;        
        
        [NotNull] public readonly KafkaConsumerBrokerPartition BrokerPartition;        

        public long TotalClientCommitedCount => _totalClientCommitedCount;
        public DateTime? ClientCommitTimestampUtc { get; private set; }

        public DateTime? CommitServerOffsetTimestampUtc => BrokerPartition.CommitServerOffsetTimestampUtc;        
                        
        private long _totalClientCommitedCount;

        public KafkaConsumerTopicPartition([NotNull] string topicName, int partitionId,
            [NotNull] KafkaConsumerGroupData group,
            [NotNull] KafkaConsumerSettings settings,
            [CanBeNull] IKafkaConsumerFallbackHandler fallbackHandler,
            [CanBeNull] IKafkaConsumerTopicLogger logger)
        {            
            PartitonId = partitionId;            
            BrokerPartition = new KafkaConsumerBrokerPartition(topicName, PartitonId, group, settings, fallbackHandler, logger);  
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
