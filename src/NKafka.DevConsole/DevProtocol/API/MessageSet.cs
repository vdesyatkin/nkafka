using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class MessageSet
    {
        public MessageCodec Codec { get; set; }

        public IReadOnlyList<MessageAndOffset> Messages { get; set; }
    }
}
