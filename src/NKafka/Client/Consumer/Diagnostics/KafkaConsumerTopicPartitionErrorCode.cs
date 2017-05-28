using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{
    [PublicAPI]
    public enum KafkaConsumerTopicPartitionErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        TransportError = -2,
        ProtocolError = -3,
        ClientTimeout = -4,
        ClientError = -5,

        HostUnreachable = -6,
        HostNotAvailable = -7,
        NotAuthorized = -8,


        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// If replica is expected on a broker, but is not (this can be safely ignored).
        /// </summary>
        ReplicaNotAvailable = 9
    }
}