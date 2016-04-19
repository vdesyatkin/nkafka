﻿using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer
{
    [PublicAPI]
    public sealed class KafkaProducerFallbackInfo
    {
        [NotNull] public readonly string TopicName;

        public readonly int PartitionId;

        public readonly DateTime TimestampUtc;

        [NotNull] public readonly KafkaMessage Message;

        public readonly KafkaProdcuerFallbackErrorCode Reason;

        public KafkaProducerFallbackInfo([NotNull] string topicName, int partitionId, DateTime timestampUtc, 
            [NotNull] KafkaMessage message, KafkaProdcuerFallbackErrorCode reason)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            TimestampUtc = timestampUtc;
            Message = message;
            Reason = reason;
        }
    }

    [PublicAPI]
    public sealed class KafkaProducerFallbackInfo<TKey, TData>
    {
        [NotNull]
        public readonly string TopicName;

        public readonly int PartitionId;

        public readonly DateTime TimestampUtc;

        [NotNull]
        public readonly KafkaMessage<TKey, TData> Message;

        public readonly KafkaProdcuerFallbackErrorCode Reason;

        public KafkaProducerFallbackInfo([NotNull] string topicName, int partitionId, DateTime timestampUtc,
            [NotNull] KafkaMessage<TKey, TData> message, KafkaProdcuerFallbackErrorCode reason)
        {
            TopicName = topicName;
            PartitionId = partitionId;
            TimestampUtc = timestampUtc;
            Message = message;
            Reason = reason;
        }
    }
}