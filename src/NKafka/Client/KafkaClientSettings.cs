using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;

namespace NKafka.Client
{
    public sealed class KafkaClientSettings
    {
        public readonly KafkaVersion KafkaVersion;
        public readonly string ClientId;
        public readonly int WorkerThreadCount;
        public readonly TimeSpan WorkerPeriod;

        public readonly IReadOnlyCollection<KafkaBrokerInfo> MetadataBrokers;
        
        public readonly KafkaConnectionSettings ConnectionSettings;

        public KafkaClientSettings(
            KafkaVersion kafkaVersion,
            [CanBeNull] string clientId,
            [NotNull, ItemNotNull] IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers,

            int workerThreadCount,
            TimeSpan workerPeriod,
                        
            [CanBeNull] KafkaConnectionSettings connectionSettings            
            )
        {
            KafkaVersion = kafkaVersion;
            ClientId = clientId;
            MetadataBrokers = metadataBrokers;
            WorkerThreadCount = workerThreadCount;
            WorkerPeriod = workerPeriod;
            ConnectionSettings = connectionSettings;            
        }
    }
}
