using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Protocol;

namespace NKafka.Client
{
    [PublicAPI]
    public sealed class KafkaClientSettings
    {
        public readonly KafkaVersion KafkaVersion;
        public readonly string ClientId;
        public readonly IReadOnlyCollection<KafkaBrokerInfo> MetadataBrokers;

        public readonly int WorkerThreadCount;
        public readonly TimeSpan WorkerPeriod;        
        public readonly TimeSpan MetadataErrorRetryPeriod;

        public readonly KafkaConnectionSettings ConnectionSettings;
        public readonly KafkaProtocolSettings ProtocolSettings;

        public KafkaClientSettings(
            KafkaVersion kafkaVersion,
            [CanBeNull] string clientId,
            [NotNull, ItemNotNull] IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers,            

            int workerThreadCount,
            TimeSpan workerPeriod,
            TimeSpan metadataErrorRetryPeriod,

            [CanBeNull] KafkaConnectionSettings connectionSettings,
            [CanBeNull] KafkaProtocolSettings protocolSettings
            )
        {
            KafkaVersion = kafkaVersion;
            ClientId = clientId;
            MetadataBrokers = metadataBrokers;
            WorkerThreadCount = workerThreadCount;
            WorkerPeriod = workerPeriod;
            MetadataErrorRetryPeriod = metadataErrorRetryPeriod;
            ConnectionSettings = connectionSettings;
            ProtocolSettings = protocolSettings;
        }
    }
}
