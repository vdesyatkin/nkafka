﻿using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup.Diagnostics
{
    [PublicAPI]
    public enum KafkaConsumerGroupErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        TransportError = -2,
        ProtocolError = -3,
        ClientTimeout = -4,
        ClientError = -5,
        AssignmentError = -6,

        HostUnreachable = -7,
        HostNotAvailable = -8,
        NotAuthorized = -9,

        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata.
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        /// The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition),<br/>
        ///  or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.
        /// </summary>
        GroupLoadInProgress = 14,

        /// <summary>
        /// The broker returns this error code for group coordinator requests, offset commits, and most group management requests <br/>
        /// if the offsets topic has not yet been created, or if the group coordinator is not active.
        /// </summary>
        GroupCoordinatorNotAvailable = 15,

        /// <summary>
        /// The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.
        /// </summary>
        NotCoordinatorForGroup = 16,

        /// <summary>
        /// Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.
        /// </summary>
        Rebalance = 22,

        /// <summary>
        /// Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        /// Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        /// Return in join group when the requested session timeout is outside of the allowed range on the broker.
        /// </summary>
        InvalidSessionTimeout = 26,

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
        GroupAuthorizationFailed = 30
    }
}