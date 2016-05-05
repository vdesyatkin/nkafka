using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicInfo
    {
        [NotNull] public readonly string TopicName;

        [NotNull] public readonly KafkaClientTopicMetadataInfo MetadataInfo;        

        public readonly bool IsReady;

        [NotNull] public readonly KafkaProducerTopicMessageCountInfo MessageCountInfo;

        [NotNull] public readonly KafkaProducerTopicMessageSizeInfo MessageSizeInfo;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaProducerTopicPartitionInfo> Partitions;

        public readonly DateTime TimestampUtc;

        public KafkaProducerTopicInfo([NotNull] string topicName, bool isReady,
            [NotNull] KafkaClientTopicMetadataInfo metadataInfo,
            [NotNull]KafkaProducerTopicMessageCountInfo messageCountInfo,
            [NotNull] KafkaProducerTopicMessageSizeInfo messageSizeInfo,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartitionInfo> partitions,
            DateTime timestampUtc)
        {
            TopicName = topicName;
            MetadataInfo = metadataInfo;            
            IsReady = isReady;            
            MessageCountInfo = messageCountInfo;
            MessageSizeInfo = messageSizeInfo;
            Partitions = partitions;
            TimestampUtc = timestampUtc;
        }
    }
}
