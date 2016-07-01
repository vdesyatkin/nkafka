using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicMessageSizeInfo
    {
        public readonly long TotalReceivedBytes;

        public readonly long TotalConsumedBytes;

        public readonly long ConsumePendingBytes;

        public readonly long BufferedBytes;

        public KafkaConsumerTopicMessageSizeInfo(long totalReceivedBytes, long totalConsumedBytes, long consumePendingBytes, long bufferedBytes)
        {
            TotalReceivedBytes = totalReceivedBytes;
            TotalConsumedBytes = totalConsumedBytes;
            ConsumePendingBytes = consumePendingBytes;
            BufferedBytes = bufferedBytes;
        }
    }
}
