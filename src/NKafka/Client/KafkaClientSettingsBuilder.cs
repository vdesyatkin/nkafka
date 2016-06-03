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
        public static readonly KafkaVersion DefaultKafkaVersion = KafkaVersion.V0_10;
        public static readonly string DefaultClientId = "nkafka";
        public static readonly int DefaultWorkerThreadCount = 1;
        public static readonly TimeSpan DefaultWorkerPeriod = TimeSpan.FromSeconds(1);
        public static readonly TimeSpan DefaultMetadataErrorRetryPeriod = TimeSpan.FromSeconds(10);

        private KafkaVersion? _kafkaVersion;
        private string _clientId;

        private int? _workerThreadCount;
        private TimeSpan? _workerPeriod;
        private TimeSpan? _metadataErrorRetryPeriod;

        [NotNull]
        private readonly List<KafkaBrokerInfo> _metadataBrokers;

        [CanBeNull]
        private KafkaConnectionSettings _connectionSettings;

        [CanBeNull]
        private KafkaProtocolSettings _protocolSettings;

        public KafkaClientSettingsBuilder(KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo>();
            if (metadataBroker == null) return;
            _metadataBrokers.Add(metadataBroker);
        }

        public KafkaClientSettingsBuilder(IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers)
        {
            metadataBrokers = metadataBrokers ?? new KafkaBrokerInfo[0];
            _metadataBrokers = new List<KafkaBrokerInfo>(metadataBrokers.Count);
            foreach (var metadataBroker in metadataBrokers)
            {
                if (metadataBroker == null) continue;
                _metadataBrokers.Add(metadataBroker);
            }
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
            var kafkaVersion = _kafkaVersion ?? DefaultKafkaVersion;
            var clientId = _clientId ?? DefaultClientId;
            var metadataBrokers = _metadataBrokers.ToArray();

            var workerThreadCount = _workerThreadCount ?? DefaultWorkerThreadCount;
            var workerPeriod = _workerPeriod ?? DefaultWorkerPeriod;
            var metadataErrorRetryPeriod = _metadataErrorRetryPeriod ?? DefaultMetadataErrorRetryPeriod;

            var connectionSettings = _connectionSettings ?? KafkaConnectionSettingsBuilder.Default;
            var protocolSettings = _protocolSettings ?? KafkaProtocolSettingsBuilder.Default;

            return new KafkaClientSettings(kafkaVersion, clientId, metadataBrokers,
                workerThreadCount, workerPeriod, metadataErrorRetryPeriod,
                connectionSettings, protocolSettings);
        }
    }
}