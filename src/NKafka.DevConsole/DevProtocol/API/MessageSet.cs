using System.Collections.Generic;

namespace NKafka.DevConsole.DevProtocol.API
{
    public class MessageSet
    {
        public MessageCodec Codec { get; set; }

        public IReadOnlyList<MessageAndOffset> Messages { get; set; }
    }
}
