using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Internal
{
    internal interface IKafkaConsumerMessageQueue
    {
        void Enqueue(IReadOnlyList<KafkaMessageAndOffset> messages);
    }
}