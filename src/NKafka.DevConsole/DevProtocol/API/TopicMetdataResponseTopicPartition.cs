using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class TopicMetdataResponseTopicPartition
    {
        /// <summary>
        /// Error code. 0 indicates no error occured.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; set; }
        /// <summary>
        /// The Id of the partition that this metadata describes.
        /// </summary>
        public int PartitionId { get; set; }
        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public int LeaderId { get; set; }
        /// <summary>
        /// The set of alive nodes that currently acts as slaves for the leader for this partition.
        /// </summary>
        public IReadOnlyList<int> Replicas { get; set; }
        /// <summary>
        /// The set subset of the replicas that are "caught up" to the leader.
        /// </summary>
        public IReadOnlyList<int> Isr { get; set; }
    }
}
