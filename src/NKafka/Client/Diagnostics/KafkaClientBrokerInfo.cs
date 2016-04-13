using System;
using JetBrains.Annotations;
using NKafka.Metadata;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientBrokerInfo
    {
        public readonly string BrokerName;

        public readonly KafkaBrokerMetadata Metadata;

        public readonly bool IsOpenned;

        public readonly KafkaClientBrokerErrorCode? Error;

        public readonly DateTime? ConnectionTimestampUtc;

        public readonly DateTime? LastActivityTimestampUtc;        

        public KafkaClientBrokerInfo(string brokerName, KafkaBrokerMetadata metadata, bool isOpenned, KafkaClientBrokerErrorCode? error, 
            DateTime? connectionTimestampUtc, DateTime? lastActivityTimestampUtc)
        {
            BrokerName = brokerName;
            Metadata = metadata;
            IsOpenned = isOpenned;
            Error = error;
            ConnectionTimestampUtc = connectionTimestampUtc;
            LastActivityTimestampUtc = lastActivityTimestampUtc;
        }
    }
}
