using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    public interface IKafkaProducerMessageQueue
    {        
        bool TryDequeueMessage(out KafkaMessage message);
        void FallbackMessage([NotNull] KafkaMessage message, DateTime timestampUtc, KafkaProdcuerFallbackErrorCode reason);
    }
}
