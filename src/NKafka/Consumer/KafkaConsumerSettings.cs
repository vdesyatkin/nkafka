using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using NKafka.Connection;

namespace NKafka.Consumer
{
    public sealed class KafkaConsumerSettings
    {
        public readonly KafkaVersion KafkaVersion;
        public readonly string ClientId;

        public readonly IReadOnlyCollection<KafkaBrokerInfo> MetadataBrokers;        

        public readonly int ConsumeThreadCount;
        public readonly TimeSpan ConsumePeriod;
        public readonly int BatchByteCountLimit;        

        public readonly TimeSpan ConsumeTimeout;

        public readonly KafkaConnectionSettings ConnectionSettings;

        public KafkaConsumerSettings(
            KafkaVersion kafkaVersion,
            [CanBeNull] string clientId,
            [NotNull, ItemNotNull] IReadOnlyCollection<KafkaBrokerInfo> metadataBrokers,
            
            int consumeThreadCount,
            TimeSpan consumePeriod,
            int batchByteCountLimit,            

            TimeSpan consumeTimeout,
            [CanBeNull] KafkaConnectionSettings connectionSettings
            )
        {
            KafkaVersion = kafkaVersion;
            ClientId = clientId;
            MetadataBrokers = metadataBrokers;            

            ConsumeThreadCount = consumeThreadCount;
            ConsumePeriod = consumePeriod;
            BatchByteCountLimit = batchByteCountLimit;            

            ConsumeTimeout = consumeTimeout;
            ConnectionSettings = connectionSettings;
        }
    }
}
