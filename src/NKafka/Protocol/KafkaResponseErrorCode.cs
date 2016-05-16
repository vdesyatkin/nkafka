﻿using JetBrains.Annotations;

namespace NKafka.Protocol
{
    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// </summary>
    [PublicAPI]
    public enum KafkaResponseErrorCode : short
    {
        /// <summary>
        /// No error - it worked!
        /// </summary>
        NoError = 0,

        /// <summary>
        /// An unexpected server error
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        /// This indicates that a message contents does not match its CRC.
        /// </summary>
        InvalidMessage = 2,

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The message has a negative size
        /// </summary>
        InvalidMessageSize = 4,

        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        /// This is not a client facing error and is used mostly by tools when a broker is not alive.
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        /// If replica is expected on a broker, but is not (this can be safely ignored).
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication.
        /// </summary>
        StaleControllerEpoch = 11,

        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata.
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        /// The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.
        /// </summary>
        GroupLoadInProgress = 14,

        /// <summary>
        /// The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.
        /// </summary>
        GroupCoordinatorNotAvailable = 15,

        /// <summary>
        /// The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.
        /// </summary>
        NotCoordinatorForGroup = 16,

        /// <summary>
        /// For a request which attempts to access an invalid topic (e.g. one which has an illegal name), or if an attempt is made to write to an internal topic (such as the consumer offsets topic).
        /// </summary>
        InvalidTopic = 17,

        /// <summary>
        /// If a message batch in a produce request exceeds the maximum configured segment size.
        /// </summary>
        RecordListTooLarge = 18,

        /// <summary>
        /// Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        /// Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        /// Returned from a produce request if the requested requiredAcks is invalid (anything other than -1, 1, or 0).
        /// </summary>
        InvalidRequiredAcks = 21,

        /// <summary>
        /// Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.
        /// </summary>
        IllegalGeneration = 22,

        /// <summary>
        /// Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        /// Returned in join group when the groupId is empty or null.
        /// </summary>
        InvalidGroupId = 24,

        /// <summary>
        /// Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        /// Return in join group when the requested session timeout is outside of the allowed range on the broker.
        /// </summary>
        InvalidSessionTimeout = 26,

        /// <summary>
        /// Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.
        /// </summary>
        RebalanceInProgress = 27,

        /// <summary>
        /// This error indicates that an offset commit was rejected because of oversize metadata.
        /// </summary>
        InvalidCommitOffsetSize = 28,

        /// <summary>
        /// Returned by the broker when the client is not authorized to access the requested topic.
        /// </summary>
        TopicAuthorizationFailed = 29,

        /// <summary>
        /// Returned by the broker when the client is not authorized to access a particular groupId.
        /// </summary>
        GroupAuthorizationFailed = 30,

        /// <summary>
        /// Returned by the broker when the client is not authorized to use an inter-broker or administrative API.
        /// </summary>
        ClusterAuthorizationFailed = 31
    }
}
