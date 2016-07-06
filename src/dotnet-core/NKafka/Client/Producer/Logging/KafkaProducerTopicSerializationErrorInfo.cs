using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicSerializationErrorInfo<TKey, TData>
    {
        [NotNull] public readonly KafkaMessage<TKey, TData> Message;

        [CanBeNull] public readonly Exception Exception;

        public KafkaProducerTopicSerializationErrorInfo([NotNull] KafkaMessage<TKey, TData> message, [CanBeNull] Exception exception)
        {
            Message = message;
            Exception = exception;
        }
    }
}
