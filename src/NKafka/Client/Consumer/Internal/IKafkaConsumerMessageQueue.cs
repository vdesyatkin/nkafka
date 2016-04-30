namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerMessageQueue
    {
        bool CanEnqueue();
        void EnqueueMessage(KafkaMessageAndOffset message);
    }
}