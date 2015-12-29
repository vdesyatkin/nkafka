using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Client.Consumer;
using NKafka.Client.Producer;
using NKafka.Connection;

namespace NKafka.Client
{
    public sealed class KafkaClientSettingsBuilder
    {
        private KafkaVersion? _kafkaVersion;
        private string _clientId;

        private int? _workerThreadCount;
        private TimeSpan? _workerPeriod;

        [NotNull] private readonly List<KafkaBrokerInfo> _metadataBrokers;
        
        [NotNull] public readonly KafkaConnectionSettingsBuilder Connection;        

        public KafkaClientSettingsBuilder([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo> { metadataBroker };
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
        public KafkaClientSettingsBuilder SetWorkerPeriod(TimeSpan workerPeriod)
        {
            _workerPeriod = workerPeriod;
            return this;
        }

        [NotNull]
        public KafkaClientSettings Build()
        {
            var kafkaVersion = _kafkaVersion ?? KafkaVersion.V0_9;
            var clientId = _clientId;
            var metadataBrokers = _metadataBrokers.ToArray();

            var workerThreadCount = _workerThreadCount ?? 0;
            var workerPeriod = _workerPeriod ?? TimeSpan.FromSeconds(1);

            var connectionSettings = Connection.Build();            

            return new KafkaClientSettings(kafkaVersion, clientId, metadataBrokers, 
                workerThreadCount, workerPeriod,
                connectionSettings);
        }        
    }
}
