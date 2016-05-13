using System;
using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Logging
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicSerializationErrorInfo
    {
        [NotNull]
        public readonly KafkaMessage Message;

        [CanBeNull]
        public readonly Exception Exception;

        public KafkaConsumerTopicSerializationErrorInfo([NotNull] KafkaMessage message, [CanBeNull] Exception exception)
        {
            Message = message;
            Exception = exception;
        }
    }
}
