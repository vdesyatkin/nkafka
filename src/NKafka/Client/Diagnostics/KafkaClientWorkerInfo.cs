using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientWorkerInfo
    {        
        public readonly int WorkerId;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientTopicInfo> Topics;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientGroupInfo> Groups;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> Brokers;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientBrokerInfo> MetadataBrokers;

        public KafkaClientWorkerInfo(int workerId,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientTopicInfo> topics, 
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientGroupInfo> groups,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> brokers,
            [NotNull, ItemNotNull]IReadOnlyList<KafkaClientBrokerInfo> metadataBrokers)
        {
            WorkerId = workerId;
            Topics = topics;
            Groups = groups;
            Brokers = brokers;
            MetadataBrokers = metadataBrokers;
        }
    }
}
