using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientWorkerInfo
    {        
        public readonly int WorkerId;        

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientTopicMetadataInfo> Topics;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientGroupMetadataInfo> Groups;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> Brokers;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> MetadataBrokers;

        public readonly DateTime TimestampUtc;

        public KafkaClientWorkerInfo(int workerId,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientTopicMetadataInfo> topics, 
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientGroupMetadataInfo> groups,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> brokers,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> metadataBrokers,
            DateTime timestampUtc)
        {
            WorkerId = workerId;            
            Topics = topics;
            Groups = groups;
            Brokers = brokers;
            MetadataBrokers = metadataBrokers;
            TimestampUtc = timestampUtc;
        }
    }
}
