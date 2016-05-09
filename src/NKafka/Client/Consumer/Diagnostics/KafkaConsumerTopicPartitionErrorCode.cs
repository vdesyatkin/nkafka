using JetBrains.Annotations;

namespace NKafka.Client.Consumer.Diagnostics
{    
    [PublicAPI]
    public enum KafkaConsumerTopicPartitionErrorCode
    {
        UnknownError = 0,

        ConnectionClosed = -1,
        ClientMaintenance = -2,
        TransportError = -3,
        ProtocolError = -4,
        ClientTimeout = -5,
        ClientError = -6,

        HostUnreachable = -7,
        HostNotAvailable = -8,
        NotAuthorized = -9,


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
