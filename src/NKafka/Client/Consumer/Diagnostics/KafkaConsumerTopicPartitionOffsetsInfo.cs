using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicPartitionOffsetsInfo
    {
        public readonly long? ReceivedClientOffset;
        public readonly long? MinAvailableServerOffset;
        public readonly long? MaxAvailableServerOffset;
        public readonly long? CatchUpGroupServerOffset;
        public readonly long? CommitedClientOffset;
        public readonly long? CommitedServerOffset;
        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicPartitionOffsetsInfo(
            long? receivedClientOffset,
            long? minAvailableOffset, long? maxAvailableServerOffset, 
            long? catchUpGroupServerOffset,
            long? commitedClientOffset, long? commitedServerOffset, 
            DateTime timestampUtc)
        {
            ReceivedClientOffset = receivedClientOffset;
            MinAvailableServerOffset = minAvailableOffset;
            CatchUpGroupServerOffset = catchUpGroupServerOffset;           
            MaxAvailableServerOffset = maxAvailableServerOffset;
            CommitedClientOffset = commitedClientOffset;
            CommitedServerOffset = commitedServerOffset;
            TimestampUtc = timestampUtc;
        }
    }
}
