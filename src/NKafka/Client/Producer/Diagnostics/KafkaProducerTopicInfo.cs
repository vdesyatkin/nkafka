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

        [NotNull] public readonly KafkaProducerTopicMessagesInfo MessagesInfo;

        [NotNull, ItemNotNull] public readonly IReadOnlyList<KafkaProducerTopicPartitionInfo> Partitions;

        public readonly DateTime TimestampUtc;

        public KafkaProducerTopicInfo([NotNull] string topicName, bool isReady,
            [NotNull] KafkaClientTopicMetadataInfo metadataInfo,
            [NotNull]KafkaProducerTopicMessagesInfo messagesInfo, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartitionInfo> partitions,
            DateTime timestampUtc)
        {
            TopicName = topicName;
            MetadataInfo = metadataInfo;
            TimestampUtc = timestampUtc;
            IsReady = isReady;            
            MessagesInfo = messagesInfo;
            Partitions = partitions;            
        }
    }
}
