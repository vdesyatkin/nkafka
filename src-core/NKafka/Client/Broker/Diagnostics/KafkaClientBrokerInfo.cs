using System;
using JetBrains.Annotations;
using NKafka.Connection.Diagnostics;
using NKafka.Metadata;

namespace NKafka.Client.Broker.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientBrokerInfo
    {
        [NotNull] public readonly string Name;        
        
        public readonly KafkaClientBrokerType BrokerType;

        [NotNull] public readonly KafkaBrokerMetadata BrokerMetadata;

        public readonly bool IsOpenned;

        public readonly KafkaBrokerStateErrorCode? Error;

        public readonly DateTime? ConnectionTimestampUtc;

        public readonly DateTime? LastActivityTimestampUtc;

        public readonly DateTime TimestampUtc;

        public KafkaClientBrokerInfo([NotNull] string name, KafkaClientBrokerType brokerType,
            [NotNull] KafkaBrokerMetadata brokerMetadata, bool isOpenned, KafkaBrokerStateErrorCode? error, 
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
