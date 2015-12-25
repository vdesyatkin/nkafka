using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;

namespace NKafka.Consumer
{
    public sealed class KafkaConsumerSettingsBuilder
    {
        private KafkaVersion? _kafkaVersion;
        private string _clientId;

        [NotNull]
        private readonly List<KafkaBrokerInfo> _metadataBrokers;        

        private int? _consumeThreadCount;
        private TimeSpan? _consumePeriod;
        private int? _batchByteCountLimit;        

        private TimeSpan? _consumeTimeout;
        private KafkaConnectionSettings _connectionSettings;

        public KafkaConsumerSettingsBuilder([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo> { metadataBroker };
        }

        [PublicAPI]
        public void SetKafkaVersion(KafkaVersion version)
        {
            _kafkaVersion = version;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetClientId([CanBeNull]string clientId)
        {
            _clientId = clientId;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder AddMetadataBroker([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers.Add(metadataBroker);
            return this;
        }
        

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumeThreadCount(int threadCount)
        {
            _consumeThreadCount = threadCount;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumePeriod(TimeSpan consumePeriod)
        {
            _consumePeriod = consumePeriod;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetBatchByteCountLimit(int batchByteCountLimit)
        {
            _batchByteCountLimit = batchByteCountLimit;
            return this;
        }       

        [PublicAPI]
        public KafkaConsumerSettingsBuilder SetConsumeTimeout(TimeSpan consumeTimeout)
        {
            _consumeTimeout = consumeTimeout;
            return this;
        }

        [PublicAPI]
        public KafkaConsumerSettingsBuilder StConnectionSettings(KafkaConnectionSettings connectionSettings)
        {
            _connectionSettings = connectionSettings;
            return this;
        }

        [NotNull]
        public KafkaConsumerSettings Build()
        {
            var kafkaVersion = _kafkaVersion ?? KafkaVersion.V0_9;
            var clientId = _clientId;
            var metadataBrokers = _metadataBrokers.ToArray();           

            var consumeThreadCount = _consumeThreadCount ?? 0;
            var consumePeriod = _consumePeriod ?? TimeSpan.FromSeconds(1);
            var batchByteCountLimit = _batchByteCountLimit ?? 200 * 200;            

            var consumeTimeout = _consumeTimeout ?? TimeSpan.FromSeconds(1);
            var connectionSettings = _connectionSettings ?? new KafkaConnectionSettings(TimeSpan.FromMinutes(30), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            return new KafkaConsumerSettings(kafkaVersion, clientId, metadataBrokers,                
                consumeThreadCount, consumePeriod, batchByteCountLimit,
                consumeTimeout, connectionSettings);
        }
    }
}
