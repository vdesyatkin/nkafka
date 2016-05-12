using System;
using JetBrains.Annotations;
using NKafka.Client.Broker;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientBrokerInfo
    {
        [NotNull] public readonly string Name;
        
        public readonly KafkaClientBrokerType BrokerType;

        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        public readonly bool IsOpenned;

        public readonly KafkaClientBrokerErrorCode? Error;

        public readonly DateTime? ConnectionTimestampUtc;

        public readonly DateTime? LastActivityTimestampUtc;

        public readonly DateTime TimestampUtc;

        public KafkaClientBrokerInfo([NotNull] string name, KafkaClientBrokerType brokerType,
            [NotNull] KafkaBrokerMetadata brokerMetadata, bool isOpenned, KafkaClientBrokerErrorCode? error, 
            DateTime? connectionTimestampUtc, DateTime? lastActivityTimestampUtc, DateTime timestampUtc)
        {
            Name = name;            
            BrokerMetadata = brokerMetadata;
            IsOpenned = isOpenned;
            Error = error;
            ConnectionTimestampUtc = connectionTimestampUtc;
            LastActivityTimestampUtc = lastActivityTimestampUtc;
            TimestampUtc = timestampUtc;
        }
    }
}
