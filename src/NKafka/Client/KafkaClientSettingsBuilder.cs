using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer;
using NKafka.Connection;
using NKafka.Producer;
using KafkaProducerSettingsBuilder = NKafka.Client.Producer.KafkaProducerSettingsBuilder;

namespace NKafka.Client
{
    public sealed class KafkaClientSettingsBuilder
    {
        private KafkaVersion? _kafkaVersion;
        private string _clientId;
        private int? _workerThreadCount;

        [NotNull] private readonly List<KafkaBrokerInfo> _metadataBrokers;
        [NotNull] public readonly KafkaProducerSettingsBuilder Producer;
        [NotNull] public readonly KafkaConsumerSettingsBuilder Consumer;
        [NotNull] public readonly KafkaConnectionSettingsBuilder Connection;

        private KafkaConnectionSettings _connectionSettings;

        public KafkaClientSettingsBuilder([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo> { metadataBroker };
            Producer = new KafkaProducerSettingsBuilder();
            Consumer = new KafkaConsumerSettingsBuilder();
            Connection = new KafkaConnectionSettingsBuilder();
        }

        [PublicAPI]
        public KafkaClientSettingsBuilder SetKafkaVersion(KafkaVersion version)
        {
            _kafkaVersion = version;
            return this;
        }

        [PublicAPI]
        public KafkaClientSettingsBuilder SetClientId([CanBeNull]string clientId)
        {
            _clientId = clientId;
            return this;
        }

        [PublicAPI]
        public KafkaClientSettingsBuilder AddMetadataBroker([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers.Add(metadataBroker);
            return this;
        }

        [PublicAPI]
        public KafkaClientSettingsBuilder SetProduceThreadCount(int threadCount)
        {
            _workerThreadCount = threadCount;
            return this;
        }

        [PublicAPI]
        public KafkaClientSettingsBuilder StConnectionSettings(KafkaConnectionSettings connectionSettings)
        {
            _connectionSettings = connectionSettings;
            return this;
        }

        [NotNull]
        public KafkaClientSettings Build()
        {
            var kafkaVersion = _kafkaVersion ?? KafkaVersion.V0_9;
            var clientId = _clientId;
            var metadataBrokers = _metadataBrokers.ToArray();
            var workerThreadCount = _workerThreadCount ?? 0;

            var connectionSettings = Connection.Build();
            var producerSettings = Producer.Build();
            var consumerSettings = Consumer.Build();

            return new KafkaClientSettings(kafkaVersion, clientId, metadataBrokers, workerThreadCount, connectionSettings, producerSettings, consumerSettings);
        }        
    }
}
