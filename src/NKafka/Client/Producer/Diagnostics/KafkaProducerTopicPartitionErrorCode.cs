using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{    
    [PublicAPI]
    public enum KafkaProducerTopicPartitionErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        ClientMaintenance = -2,        
        TransportError = -3,
        ProtocolError = -4,
        ClientTimeout = -5,        

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

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
        ServerTimeout = 7,

        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. <br/>
        /// This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        /// For a request which attempts to access an invalid topic (e.g. one which has an illegal name), <br/>
        /// or if an attempt is made to write to an internal topic (such as the consumer offsets topic).
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
        /// Returned by the broker when the client is not authorized to access the requested topic.
        /// </summary>
        TopicAuthorizationFailed = 29
    }
}
