using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Producer.Internal
{
    public interface IKafkaProducerMessageQueue
    {        
        bool TryDequeueMessage(out KafkaMessage message);

        void RollbackMessages([NotNull] IReadOnlyList<KafkaMessage> messages);
    }
}
