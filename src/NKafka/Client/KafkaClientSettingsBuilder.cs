using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;
using NKafka.Protocol;

namespace NKafka.Client
{
    [PublicAPI]
    public sealed class KafkaClientSettingsBuilder
    {
        private KafkaVersion? _kafkaVersion;
        private string _clientId;

        private int? _workerThreadCount;
        private TimeSpan? _workerPeriod;
        private TimeSpan? _metadataErrorRetryPeriod;

        [NotNull] private readonly List<KafkaBrokerInfo> _metadataBrokers;
        
        [CanBeNull] private KafkaConnectionSettings _connectionSettings;

        [CanBeNull] private KafkaProtocolSettings _protocolSettings;

        public KafkaClientSettingsBuilder([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo> { metadataBroker };
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetKafkaVersion(KafkaVersion version)
        {
            _kafkaVersion = version;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetClientId([CanBeNull]string clientId)
        {
            _clientId = clientId;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder AppendMetadataBroker([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers.Add(metadataBroker);
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetWorkerThreadCount(int threadCount)
        {
            _workerThreadCount = threadCount;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetWorkerPeriod(TimeSpan workerPeriod)
        {
            _workerPeriod = workerPeriod;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetMetadataErrorRetryPeriod(TimeSpan period)
        {
            _metadataErrorRetryPeriod = period;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetConnectionSettings([NotNull]KafkaConnectionSettings connectionSettings)
        {
            _connectionSettings = connectionSettings;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettingsBuilder SetProtocolSettings([NotNull]KafkaProtocolSettings protocolSettings)
        {
            _protocolSettings = protocolSettings;
            return this;
        }

        [PublicAPI, NotNull]
        public KafkaClientSettings Build()
        {
            var kafkaVersion = _kafkaVersion ?? KafkaVersion.V0_9;
            var clientId = _clientId;
            var metadataBrokers = _metadataBrokers.ToArray();

            var workerThreadCount = _workerThreadCount ?? 0;
            var workerPeriod = _workerPeriod ?? TimeSpan.FromSeconds(1);
            var metadataErrorRetryPeriod = _metadataErrorRetryPeriod ?? TimeSpan.FromSeconds(10);

            var connectionSettings = _connectionSettings ?? KafkaConnectionSettingsBuilder.Default;
            var protocolSettings = _protocolSettings ?? KafkaProtocolSettingsBuilder.Default;

            return new KafkaClientSettings(kafkaVersion, clientId, metadataBrokers, 
                workerThreadCount, workerPeriod, metadataErrorRetryPeriod,
                connectionSettings, protocolSettings);
        }        
    }
}
