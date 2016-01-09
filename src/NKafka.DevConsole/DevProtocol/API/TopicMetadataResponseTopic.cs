using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class TopicMetadataResponseTopic
    {
        public Int16 ErrorCode { get; set; }
        public string TopicName { get; set; }
        public IReadOnlyList<TopicMetdataResponseTopicPartition> Partitions { get; set; }
    }
}
