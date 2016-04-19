using JetBrains.Annotations;

namespace NKafka.Client.Producer.Diagnostics
{    
    [PublicAPI]
    public enum KafkaProducerTopicPartitionErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        ClientMaintenance = -2,
        ServerMaintenance = -3,
        TransportError = -4,
        ProtocolError = -5,
        ClientTimeout = -6,        

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,        

        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        ServerTimetout = 7,

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
        /// Returned from a produce request when the number of in-sync replicas is lower than the configured minimum and requiredAcks is -1.
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        /// Returned from a produce request when the message was written to the log, but with fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        /// Returned by the broker when the client is not authorized to access the requested topic.
        /// </summary>
        TopicAuthorizationFailed = 29
    }
}
