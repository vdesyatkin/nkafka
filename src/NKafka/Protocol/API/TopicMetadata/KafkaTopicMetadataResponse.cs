using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// The response contains metadata for each partition, with partitions grouped together by topic.<br/>
    /// This metadata refers to brokers by their broker id. The brokers each have a host and port.
    /// </summary>
    [PublicAPI]
    public sealed class KafkaTopicMetadataResponse : IKafkaResponse
    {
        /// <summary>
        /// The cluster id that this broker belongs to.
        /// </summary>
        public readonly string ClusterId;

        /// <summary>
        /// The broker id of the controller broker.
        /// </summary>
        public readonly int? ControllerBrokerId;

        /// <summary>
        /// Brokers metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseBroker> Brokers;

        /// <summary>
        /// Topics metadata.
        /// </summary>
        public readonly IReadOnlyList<KafkaTopicMetadataResponseTopic> Topics;

        /// <param name="clusterId">The cluster id that this broker belongs to.</param>
        /// <param name="controllerBrokerId">The broker id of the controller broker.</param>
        /// <param name="brokers">Brokers metadata.</param>
        /// <param name="topics">Topics metadata.</param>
        public KafkaTopicMetadataResponse(
            string clusterId,
            int? controllerBrokerId,            
            IReadOnlyList<KafkaTopicMetadataResponseBroker> brokers,
            IReadOnlyList<KafkaTopicMetadataResponseTopic> topics)
        {
            ClusterId = clusterId;
            ControllerBrokerId = controllerBrokerId;
            Brokers = brokers;
            Topics = topics;
        }
    }
}
