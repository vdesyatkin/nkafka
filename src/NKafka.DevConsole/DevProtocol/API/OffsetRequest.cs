using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class OffsetRequest
    {
        /// <summary>
        /// The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. 
        /// Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
        /// </summary>
        public ReplicaId ReplicaId { get; set; }

        public IReadOnlyList<OffsetRequestTopic> Topics { get; set; }        
    }
}
