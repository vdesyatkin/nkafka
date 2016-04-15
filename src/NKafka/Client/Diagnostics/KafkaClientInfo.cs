using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientInfo
    {
        public readonly DateTime TimestampUtc;

        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientWorkerInfo> Workers;
        
        public KafkaClientInfo(DateTime timestampUtc, [NotNull, ItemNotNull]IReadOnlyList<KafkaClientWorkerInfo> workers)
        {
            TimestampUtc = timestampUtc;
            Workers = workers;
        }
    }
}
