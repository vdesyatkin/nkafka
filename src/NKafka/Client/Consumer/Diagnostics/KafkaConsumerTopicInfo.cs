using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicInfo
    {
        [NotNull] public readonly string TopicName;

        public readonly bool IsReady;

        public readonly bool IsSynchronized;

        [NotNull] public readonly KafkaClientTopicMetadataInfo MetadataInfo;

        [CanBeNull] public readonly KafkaConsumerGroupInfo ConsumerGroupInfo;

        [CanBeNull] public readonly KafkaConsumerGroupInfo CatchUpGroupInfo;

        [NotNull] public readonly KafkaConsumerTopicMessageCountInfo MessageCountInfo;

        [NotNull] public readonly KafkaConsumerTopicMessageSizeInfo MessageSizeInfo;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerTopicPartitionInfo> Partitions;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicInfo([NotNull] string topicName, 
            bool isReady, bool isSynchronized,
            [NotNull] KafkaClientTopicMetadataInfo metadataInfo,
            [CanBeNull] KafkaConsumerGroupInfo consumerGroupInfo, [CanBeNull] KafkaConsumerGroupInfo catchUpGroupInfo,
            [NotNull] KafkaConsumerTopicMessageCountInfo messageCountInfo,
            [NotNull] KafkaConsumerTopicMessageSizeInfo messageSizeInfo,
            [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerTopicPartitionInfo> partitions, 
            DateTime timestampUtc)
        {
            TopicName = topicName;
            IsReady = isReady;
            IsSynchronized = isSynchronized;
            MetadataInfo = metadataInfo;
            ConsumerGroupInfo = consumerGroupInfo;
            CatchUpGroupInfo = catchUpGroupInfo;
            MessageCountInfo = messageCountInfo;
            MessageSizeInfo = messageSizeInfo;
            Partitions = partitions;
            TimestampUtc = timestampUtc;
        }
    }
}
