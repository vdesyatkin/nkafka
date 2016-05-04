using System;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroupObserver.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerGroupObserverOffsetsPartitionInfo
    {
        public readonly int PartitionId;        

        public readonly long? ServerOffset;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerGroupObserverOffsetsPartitionInfo(int partitionId, long? serverOffset, DateTime timestampUtc)
        {
            PartitionId = partitionId;            
            ServerOffset = serverOffset;
            TimestampUtc = timestampUtc;
        }
    }
}
