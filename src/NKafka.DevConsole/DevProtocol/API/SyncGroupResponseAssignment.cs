using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class SyncGroupResponseAssignment
    {        
        public short ProtocolVersion { get; set; }

        public IReadOnlyList<SyncGroupResponseAssignmentTopic> AssignedTopics { get; set; }

        public byte[] CustomData { get; set; }
    }
}
