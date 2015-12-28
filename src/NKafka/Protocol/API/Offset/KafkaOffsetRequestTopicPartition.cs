using System;

namespace NKafka.Protocol.API.Offset
{
    internal sealed class KafkaOffsetRequestTopicPartition
    {
        public readonly int PartitionId;

        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        /// Specify -1 (or null) to receive the latest offset (i.e. the offset of the next coming message) 
        /// and -2 to receive the earliest available offset. Note that because offsets are pulled in descending order, 
        /// asking for the earliest offset will always return you a single element.
        /// </summary>
        public readonly TimeSpan? Period;

        public readonly int MaxNumberOfOffsets;

        public KafkaOffsetRequestTopicPartition(int partitionId, TimeSpan? period, int maxNumberOfOffsets)
        {
            PartitionId = partitionId;
            Period = period;
            MaxNumberOfOffsets = maxNumberOfOffsets;
        }
    }
}
