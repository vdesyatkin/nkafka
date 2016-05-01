using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicPartitionOffsetsInfo
    {
        public readonly long? ReceivedOffset;
        public readonly long? MinAvailableOffset;
        public readonly long? MaxAvailableOffset;
        public readonly long? CommitedClientOffset;
        public readonly long? CommitedServerOffset;
        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicPartitionOffsetsInfo(long? receivedOffset, 
            long? minAvailableOffset, long? maxAvailableOffset, 
            long? commitedClientOffset, long? commitedServerOffset, 
            DateTime timestampUtc)
        {
            ReceivedOffset = receivedOffset;           
            MaxAvailableOffset = maxAvailableOffset;
            CommitedClientOffset = commitedClientOffset;
            CommitedServerOffset = commitedServerOffset;
            TimestampUtc = timestampUtc;
        }
    }
}
