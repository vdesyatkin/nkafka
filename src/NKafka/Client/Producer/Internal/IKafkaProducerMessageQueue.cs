using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Internal
{
    public interface IKafkaProducerMessageQueue
    {
        bool TryPeekMessage(out KafkaMessage message);
        bool TryDequeueMessage(out KafkaMessage message);
        void FallbackMessage([NotNull] KafkaMessage message, DateTime timestampUtc, KafkaProdcuerFallbackReason reason);
    }
}
