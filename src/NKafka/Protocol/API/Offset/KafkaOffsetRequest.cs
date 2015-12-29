using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Offset
{
    /// <summary>
    /// This API describes the valid offset range available for a set of topic-partitions. 
    /// As with the produce and fetch APIs requests must be directed to the broker that is currently the leader for the partitions in question. This can be determined using the metadata API.
    /// </summary>
    [PublicAPI]
    internal sealed class KafkaOffsetRequest : IKafkaRequest
    {
        /// <summary>
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 (or null) as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// </summary>
        public readonly int? ReplicaId;

        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaOffsetRequestTopic> Topics;

        public KafkaOffsetRequest(IReadOnlyList<KafkaOffsetRequestTopic> topics)
        {
            Topics = topics;
            ReplicaId = null;
        }

        public KafkaOffsetRequest(int replicaId, IReadOnlyList<KafkaOffsetRequestTopic> topics)
        {
            ReplicaId = replicaId;
            Topics = topics;            
        }
    }
}
