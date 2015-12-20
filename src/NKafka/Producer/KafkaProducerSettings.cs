using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Producer
{
    public sealed class KafkaProducerSettings
    {
        public readonly KafkaVersion KafkaVersion;
        public readonly string ClientId;
        
        public readonly IReadOnlyCollection<KafkaBrokerInfo> MetadataBrokers;

        public readonly KafkaConsistencyLevel ConsistencyLevel;
        public readonly KafkaCodecType CodecType;

        public readonly int ProduceThreadCount;
        public readonly TimeSpan ProducePeriod;
        public readonly int BatchByteCountLimit;
        public readonly int BatchMessageCountLimit;

        public readonly TimeSpan ProduceTimeout;        

        public KafkaProducerSettings(
            KafkaVersion kafkaVersion,
            [CanBeNull] string clientId,
            [NotNull, ItemNotNull] IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers,
            
            KafkaConsistencyLevel consistencyLevel,
            KafkaCodecType codecType,            
            
            int produceThreadCount,
            TimeSpan producePeriod,
            int batchByteCountLimit,
            int batchMessageCountLimit,

            TimeSpan produceTimeout
            )
        {
            KafkaVersion = kafkaVersion;
            ClientId = clientId;
            MetadataBrokers = metadataBrokers;

            ConsistencyLevel = consistencyLevel;
            CodecType = codecType;

            ProduceThreadCount = produceThreadCount;
            ProducePeriod = producePeriod;
            BatchByteCountLimit = batchByteCountLimit;
            BatchMessageCountLimit = batchMessageCountLimit;

            ProduceTimeout = produceTimeout;            
        }
    }
}
