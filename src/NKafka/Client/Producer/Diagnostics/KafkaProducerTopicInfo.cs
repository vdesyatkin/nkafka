using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Diagnostics;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaProducerTopicInfo
    {
        [NotNull]
        public readonly string TopicName;

        [NotNull]
        public readonly KafkaClientTopicInfo TopicInfo;

        public readonly DateTime TimestampUtc;

        public readonly bool IsReady;        

        [NotNull]
        public readonly KafkaProducerTopicMessageCountInfo MessagesInfo;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaProducerTopicPartitionInfo> Partitions;                   

        public KafkaProducerTopicInfo([NotNull] string topicName, [NotNull] KafkaClientTopicInfo topicInfo, DateTime timestampUtc, 
            bool isReady,
            [NotNull]KafkaProducerTopicMessageCountInfo messagesInfo, 
            [NotNull, ItemNotNull] IReadOnlyList<KafkaProducerTopicPartitionInfo> partitions)
        {
            TopicName = topicName;
            TopicInfo = topicInfo;
            TimestampUtc = timestampUtc;
            IsReady = isReady;            
            MessagesInfo = messagesInfo;
            Partitions = partitions;            
        }
    }
}
