using System;
using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class TopicMetadataResponseTopic
    {
        public Int16 ErrorCode { get; set; }
        public string TopicName { get; set; }
        public IReadOnlyList<TopicMetdataResponseTopicPartition> Partitions { get; set; }
    }
}
