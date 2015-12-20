using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class JoinGroupResponseMember
    {
        public string MemberId { get; set; }

        public short ProtocolVersion { get; set; }        

        public IReadOnlyList<string> TopicNames { get; set; }

        public IReadOnlyList<string> AssignmentStrategies { get; set; }

        public byte[] CustomData { get; set; }
    }
}
