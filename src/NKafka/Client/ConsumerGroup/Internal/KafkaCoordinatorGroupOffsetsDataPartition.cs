using System;
using NKafka.Client.Consumer.Internal;

namespace NKafka.Client.ConsumerGroup.Internal
{    
    internal sealed class KafkaCoordinatorGroupOffsetsDataPartition : IKafkaConsumerCoordinatorOffsetsData
    {
        public long? GroupClientOffset;

        public long? GroupServerOffset;

        public DateTime TimestampUtc;
        
        long? IKafkaConsumerCoordinatorOffsetsData.GroupServerOffset => GroupServerOffset;

        public KafkaCoordinatorGroupOffsetsDataPartition(long? groupClientOffset, long? groupServerOffset, DateTime timestampUtc)
        {
            GroupClientOffset = groupClientOffset;
            GroupServerOffset = groupServerOffset;
            TimestampUtc = timestampUtc;
        }                
    }
}
