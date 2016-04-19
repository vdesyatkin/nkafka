namespace NKafka.Client.Producer.Internal
{
    public interface IKafkaProducerMessageQueue
    {
        bool TryPeekMessage(out KafkaMessage message);
        bool TryDequeueMessage(out KafkaMessage message);
    }
}
