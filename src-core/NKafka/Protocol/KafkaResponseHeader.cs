namespace NKafka.Protocol
{
    public sealed class KafkaResponseHeader
    {
        public readonly int DataSize;
        public readonly int CorrelationId;

        public KafkaResponseHeader(int dataSize, int correlationId)
        {
            DataSize = dataSize;
            CorrelationId = correlationId;
        }
    }
}
