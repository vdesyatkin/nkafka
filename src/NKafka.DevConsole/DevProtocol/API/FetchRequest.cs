using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class FetchRequest
    {
        /// <summary>
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// </summary>
        public ReplicaId ReplicaId { get; set; }

        /// <summary>
        /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public int MaxWaitTimeMs { get; set; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response. 
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. 
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. 
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data 
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinBytes { get; set; }

        public IReadOnlyList<FetchRequestTopic> Topics { get; set; }
    }
}
