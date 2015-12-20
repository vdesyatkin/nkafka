using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Producer
{
    public sealed class KafkaProducerSettingsBuilder
    {
        private KafkaVersion? _kafkaVersion;
        private string _clientId;

        [NotNull] private readonly List<KafkaBrokerInfo> _metadataBrokers;

        private KafkaConsistencyLevel? _consistencyLevel;
        private KafkaCodecType? _codecType;

        private int? _produceThreadCount;
        private TimeSpan? _producePeriod;
        private int? _batchByteCountLimit;
        private int? _batchMessageCountLimit;

        private TimeSpan? _produceTimeout;        

        public KafkaProducerSettingsBuilder([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers = new List<KafkaBrokerInfo> {metadataBroker};
        }

        [PublicAPI]
        public void SetKafkaVersion(KafkaVersion version)
        {
            _kafkaVersion = version;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetClientId([CanBeNull]string clientId)
        {
            _clientId = clientId;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder AddMetadataBroker([NotNull] KafkaBrokerInfo metadataBroker)
        {
            _metadataBrokers.Add(metadataBroker);
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetConsistencyLevel(KafkaConsistencyLevel consistencyLevel)
        {
            _consistencyLevel = consistencyLevel;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetCodecType(KafkaCodecType codecType)
        {
            _codecType = codecType;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProduceThreadCount(int threadCount)
        {
            _produceThreadCount = threadCount;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProducePeriod(TimeSpan producePeriod)
        {
            _producePeriod = producePeriod;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetBatchByteCountLimit(int batchByteCountLimit)
        {
            _batchByteCountLimit = batchByteCountLimit;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetBatchMessageCountLimit(int batchMessageCountLimit)
        {
            _batchMessageCountLimit = batchMessageCountLimit;
            return this;
        }

        [PublicAPI]
        public KafkaProducerSettingsBuilder SetProduceTimeout(TimeSpan produceTimeout)
        {
            _produceTimeout = produceTimeout;
            return this;
        }      

        [NotNull]
        public KafkaProducerSettings Build()
        {
            var kafkaVersion = _kafkaVersion ?? KafkaVersion.V0_9;
            var clientId = _clientId;
            var metadataBrokers = _metadataBrokers.ToArray();

            var consistencyLevel = _consistencyLevel ?? KafkaConsistencyLevel.OneReplica;
            var codecType = _codecType ?? KafkaCodecType.CodecNone;

            var produceThreadCount = _produceThreadCount ?? 0;
            var producePeriod = _producePeriod ?? TimeSpan.FromSeconds(1);
            var batchByteCountLimit = _batchByteCountLimit ?? 200 * 200;
            var batchMessageCountLimit = _batchMessageCountLimit ?? 200;

            var produceTimeout = _produceTimeout ?? TimeSpan.FromSeconds(1);            

            return new KafkaProducerSettings(kafkaVersion, clientId, metadataBrokers,
                consistencyLevel, codecType,
                produceThreadCount, producePeriod, batchByteCountLimit, batchMessageCountLimit,
                produceTimeout);
        }
    }
}
