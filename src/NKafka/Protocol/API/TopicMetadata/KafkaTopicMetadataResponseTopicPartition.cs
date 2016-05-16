using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.TopicMetadata
{
    /// <summary>
    /// Topic partition metadata.
    /// </summary>
    [PublicAPI]
    public sealed class KafkaTopicMetadataResponseTopicPartition
    {
        /// <summary>
        /// Error code.
        /// </summary>
        public readonly KafkaResponseErrorCode ErrorCode;

        /// <summary>
        /// The Id of the partition that this metadata describes.
        /// </summary>
        public readonly int PartitionId;

        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. <br/>
        /// If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public readonly int LeaderId;

        /// <summary>
        /// The set of alive nodes that currently acts as slaves for the leader for this partition.
        /// </summary>
        public readonly IReadOnlyList<int> ReplicaIds;

        /// <summary>
        /// The set subset of the replicas that are "caught up" to the leader.
        /// </summary>
        public readonly IReadOnlyList<int> Isr;

        /// <param name="errorCode">Error code.</param>
        /// <param name="partitionId">The Id of the partition that this metadata describes.</param>        
        /// <param name="leaderId">
        /// The node id for the kafka broker currently acting as leader for this partition.<br/>
        /// If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </param>
        /// <param name="replicaIds">The set of alive nodes that currently acts as slaves for the leader for this partition.</param>
        /// <param name="isr">The set subset of the replicas that are "caught up" to the leader.</param>
        public KafkaTopicMetadataResponseTopicPartition(KafkaResponseErrorCode errorCode, int partitionId, int leaderId,
            IReadOnlyList<int> replicaIds, IReadOnlyList<int> isr)
        {
            PartitionId = partitionId;
            ErrorCode = errorCode;
            LeaderId = leaderId;
            ReplicaIds = replicaIds;
            Isr = isr;            
        }
    }
}
