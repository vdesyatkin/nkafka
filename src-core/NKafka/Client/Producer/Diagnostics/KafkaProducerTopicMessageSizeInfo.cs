using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicMessageSizeInfo
    {
        public readonly long TotalEnqueuedBytes;

        public readonly long TotalSentBytes;

        public readonly long SendPendingBytes;

        public KafkaProducerTopicMessageSizeInfo(long totalEnqueuedBytes, long totalSentBytes, long sendPendingBytes)
        {
            TotalEnqueuedBytes = totalEnqueuedBytes;
            TotalSentBytes = totalSentBytes;
            SendPendingBytes = sendPendingBytes;
        }
    }
}
