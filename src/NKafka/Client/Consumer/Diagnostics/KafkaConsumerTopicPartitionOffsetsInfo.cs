using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicPartitionOffsetsInfo
    {
        public readonly long? ReceivedOffset;        
        public readonly long? AvailableOffset;
        public readonly long? CommitedClientOffset;
        public readonly long? CommitedServerOffset;
        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicPartitionOffsetsInfo(long? receivedOffset, long? availableOffset, long? commitedClientOffset, long? commitedServerOffset, DateTime timestampUtc)
        {
            ReceivedOffset = receivedOffset;            
            AvailableOffset = availableOffset;
            CommitedClientOffset = commitedClientOffset;
            CommitedServerOffset = commitedServerOffset;
            TimestampUtc = timestampUtc;
        }
    }
}
