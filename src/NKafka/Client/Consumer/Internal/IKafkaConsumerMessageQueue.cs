using System.Collections.Generic;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerMessageQueue
    {
        void Enqueue(IReadOnlyList<KafkaMessageAndOffset> messages);
    }
}