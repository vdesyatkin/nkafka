using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class JoinGroupRequestProtocol
    {
        public string ProtocolName { get; set; }

        public short Version { get; set; }

        public IReadOnlyList<string> TopicNames { get; set; }

        public IReadOnlyList<string> AssignmentStrategies { get; set; }

        public byte[] CustomData { get; set; }
    }
}
