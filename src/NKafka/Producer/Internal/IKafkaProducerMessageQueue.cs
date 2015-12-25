namespace NKafka.Producer.Internal
{
    public interface IKafkaProducerMessageQueue
    {        
        bool TryDequeueMessage(out KafkaMessage message);        
    }
}
