using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer;
using NKafka.Client.Producer;
using NKafka.Connection;

namespace NKafka.Client
{
    public sealed class KafkaClientSettings
    {
        public readonly KafkaVersion KafkaVersion;
        public readonly string ClientId;
        public readonly int WorkerThreadCount;

        public readonly IReadOnlyCollection<KafkaBrokerInfo> MetadataBrokers;

        public readonly KafkaProducerSettings ProducerSettings;
        public readonly KafkaConsumerSettings ConsumerSettings;        
        public readonly KafkaConnectionSettings ConnectionSettings;

        public KafkaClientSettings(
            KafkaVersion kafkaVersion,
            [CanBeNull] string clientId,
            [NotNull, ItemNotNull] IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers,

            int workerThreadCount,
                        
            [CanBeNull] KafkaConnectionSettings connectionSettings,
            [CanBeNull] KafkaProducerSettings producerSettings,
            [CanBeNull] KafkaConsumerSettings consumerSettings
            )
        {
            KafkaVersion = kafkaVersion;
            ClientId = clientId;
            MetadataBrokers = metadataBrokers;
            WorkerThreadCount = workerThreadCount;
            ConnectionSettings = connectionSettings;
            ProducerSettings = producerSettings;
            ConsumerSettings = consumerSettings;
        }
    }
}
