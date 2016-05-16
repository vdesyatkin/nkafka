using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.Fetch
{
    /// <summary>
    /// <para>
    /// The fetch API is used to fetch a chunk of one or more logs for some topic-partitions. <br/>
    /// Logically one specifies the topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages. <br/>
    /// In general, the return messages will have offsets larger than or equal to the starting offset. <br/>
    /// However, with compressed messages, it's possible for the returned messages to have offsets smaller than the starting offset. <br/>
    /// The number of such messages is typically small and the caller is responsible for filtering out those messages.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <para>
    /// Fetch requests follow a long poll model so they can be made to block for a period of time if sufficient data is not immediately available.<br/>    
    /// As an optimization the server is allowed to return a partial message at the end of the message set. Clients should handle this case.
    /// </para>
    /// </remarks>
    [PublicAPI]
    public sealed class KafkaFetchRequest : IKafkaRequest
    {
        /// <summary>
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 (or null) as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// </summary>
        public readonly int? ReplicaId;

        /// <summary>
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public readonly TimeSpan MaxWaitTime;

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response. 
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. 
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. 
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data 
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public readonly int MinBytes;

        /// <summary>
        /// Topics.
        /// </summary>
        public readonly IReadOnlyList<KafkaFetchRequestTopic> Topics;

        public KafkaFetchRequest(TimeSpan maxWaitTime, int minBytes, IReadOnlyList<KafkaFetchRequestTopic> topics)
        {
            MaxWaitTime = maxWaitTime;
            MinBytes = minBytes;
            Topics = topics;
        }

        public KafkaFetchRequest(int replicaId, TimeSpan maxWaitTime, int minBytes, IReadOnlyList<KafkaFetchRequestTopic> topics)
            : this(maxWaitTime, minBytes, topics)
        {
            ReplicaId = replicaId;
        }
    }
}
