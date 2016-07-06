using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Internal
{    
    internal sealed class KafkaCoordinatorGroupOffsetsData
    {
        [NotNull] public readonly IReadOnlyDictionary<string, KafkaCoordinatorGroupOffsetsDataTopic> Topics;

        public readonly DateTime TimestampUtc;

        public KafkaCoordinatorGroupOffsetsData([NotNull] IReadOnlyDictionary<string, KafkaCoordinatorGroupOffsetsDataTopic> topics,
            DateTime timestampUtc)
        {
            Topics = topics;
            TimestampUtc = timestampUtc;
        }
    }
}
