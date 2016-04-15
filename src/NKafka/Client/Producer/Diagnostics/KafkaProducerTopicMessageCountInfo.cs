using System;
using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{
    [PublicAPI]
    public struct KafkaProducerTopicMessageCountInfo
    {
        public readonly long MessageCount;

        public readonly DateTime TimestampUtc;

        public KafkaProducerTopicMessageCountInfo(long messageCount, DateTime timestampUtc)
        {
            MessageCount = messageCount;
            TimestampUtc = timestampUtc;
        }
    }
}
