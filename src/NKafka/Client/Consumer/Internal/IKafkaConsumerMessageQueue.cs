using System.Collections.Generic;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerMessageQueue
    {
        bool CanEnqueue();
        void Enqueue(IReadOnlyList<KafkaMessageAndOffset> messages);
    }
}