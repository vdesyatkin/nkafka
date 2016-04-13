using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.Diagnostics
{
    [PublicAPI]
    public sealed class KafkaClientInfo
    {
        [NotNull, ItemNotNull]
        public readonly IReadOnlyList<KafkaClientWorkerInfo> Workers;
        
        public KafkaClientInfo([NotNull, ItemNotNull]IReadOnlyList<KafkaClientWorkerInfo> workers)
        {
            Workers = workers;
        }
    }
}
