using System.Collections.Generic;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// The response contains metadata for each partition, with partitions grouped together by topic.<br/>
    /// This metadata refers to brokers by their broker id. The brokers each have a host and port.
    /// </summary>
    internal sealed class KafkaTopicMetadataResponse : IKafkaResponse
    {
        /// <summary>
        /// Brokers metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseBroker> Brokers;

        /// <summary>
        /// Topics metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseTopic> Topics;
        
        /// <param name="brokers">Brokers metadata.</param>
        /// <param name="topics">Topics metadata.</param>
        public KafkaTopicMetadataResponse(
            IReadOnlyList<KafkaTopicMetadataResponseBroker> brokers,
            IReadOnlyList<KafkaTopicMetadataResponseTopic> topics)
        {            
            Brokers = brokers;
            Topics = topics;
        }
    }
}
