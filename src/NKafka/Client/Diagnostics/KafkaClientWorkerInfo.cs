﻿using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientWorkerInfo
    {        
        public readonly int WorkerId;

        public readonly DateTime TimestampUtc;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientTopicMetadataInfo> Topics;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientGroupMetadataInfo> Groups;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> Brokers;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> MetadataBrokers;

        public KafkaClientWorkerInfo(int workerId, DateTime timestampUtc,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientTopicMetadataInfo> topics, 
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientGroupMetadataInfo> groups,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> brokers,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> metadataBrokers)
        {
            WorkerId = workerId;
            TimestampUtc = timestampUtc;
            Topics = topics;
            Groups = groups;
            Brokers = brokers;
            MetadataBrokers = metadataBrokers;            
        }
    }
}
