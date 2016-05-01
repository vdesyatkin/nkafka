using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaConsumerTopicInfo
    {
        [NotNull] public readonly string TopicName;

        public readonly bool IsReady;

        [NotNull] public readonly KafkaClientTopicMetadataInfo MetadataInfo;

        [NotNull] public readonly KafkaConsumerTopicMessagesInfo MessagesInfo;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaConsumerTopicPartitionInfo> Partitions;

        public readonly DateTime TimestampUtc;

        public KafkaConsumerTopicInfo([NotNull] string topicName, bool isReady, 
            [NotNull] KafkaClientTopicMetadataInfo metadataInfo, 
            [NotNull] KafkaConsumerTopicMessagesInfo messagesInfo, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaConsumerTopicPartitionInfo> partitions, 
            DateTime timestampUtc)
        {
            TopicName = topicName;
            IsReady = isReady;
            MetadataInfo = metadataInfo;
            MessagesInfo = messagesInfo;
            Partitions = partitions;
            TimestampUtc = timestampUtc;
        }
    }
}
