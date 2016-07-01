using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Logging
{
    [PublicAPI]
    public sealed class KafkaProducerTopicPartitioningErrorInfo
    {
        [NotNull] public readonly KafkaMessage Message;

        [NotNull] public readonly IReadOnlyList<int> PartitionIds;        

        [CanBeNull] public readonly Exception Exception;

        public KafkaProducerTopicPartitioningErrorInfo([NotNull] KafkaMessage message, [NotNull] IReadOnlyList<int> partitionIds, [CanBeNull] Exception exception)
        {
            Message = message;
            PartitionIds = partitionIds;
            Exception = exception;
        }
    }

    [PublicAPI]
    public sealed class KafkaProducerTopicPartitioningErrorInfo<TKey, TData>
    {
        [NotNull]
        public readonly KafkaMessage<TKey, TData> Message;

        [NotNull]
        public readonly IReadOnlyList<int> PartitionIds;

        [CanBeNull]
        public readonly Exception Exception;

        public KafkaProducerTopicPartitioningErrorInfo([NotNull] KafkaMessage<TKey, TData> message, [NotNull] IReadOnlyList<int> partitionIds, [CanBeNull] Exception exception)
        {
            Message = message;
            PartitionIds = partitionIds;
            Exception = exception;
        }
    }
}
