using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// <para>
    /// This API answers the following questions:<br/>
    /// - What topics exist?<br/>
    /// - How many partitions does each topic have?<br/>
    /// - Which broker is currently the leader for each partition?<br/>
    /// - What is the host and port for each of these brokers?
    /// </para>
    /// <para>
    /// This is the only request that can be addressed to any broker in the cluster.<br/>
    /// Since there may be many topics the client can give an optional list of topic names in order to only return metadata for a subset of topics.<br/>
    /// The metadata returned is at the partition level, but grouped together by topic for convenience and to avoid redundancy.<br/>
    /// For each partition the metadata contains the information for the leader as well as for all the replicas and the list of replicas that are currently in-sync.<br/>
    /// </para>
    /// </summary>
    /// <remarks>
    /// Note: If "auto.create.topics.enable" is set in the broker configuration, a topic metadata request will create the topic with the default replication factor and number of partitions.
    /// </remarks>   
    [PublicAPI]
    public sealed class KafkaTopicMetadataRequest : IKafkaRequest
    {
        /// <summary>
        /// The topics to produce metadata for. If empty the request will yield metadata for all topics.
        /// </summary>
        public readonly IReadOnlyList<string> TopicNames;
        
        /// <param name="topicNames">The topics to produce metadata for. If empty the request will yield metadata for all topics.</param>
        public KafkaTopicMetadataRequest(IReadOnlyList<string> topicNames)
        {
            TopicNames = topicNames;            
        }
    }
}
