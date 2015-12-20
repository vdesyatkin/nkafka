using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class SyncGroupResponseAssignment
    {        
        public short ProtocolVersion { get; set; }

        public IReadOnlyList<SyncGroupResponseAssignmentTopic> AssignedTopics { get; set; }

        public byte[] CustomData { get; set; }
    }
}
