using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.DevConsole.DevProtocol.API
{
    [PublicAPI]
    public class JoinGroupRequest
    {
        public string GroupId { get; set; }

        public string MemberId { get; set; }        

        /// <summary>
        /// 6-30 seconds by default
        /// </summary>
        public KafkaTimeout SessionTimeout { get; set; }
        
        public IReadOnlyList<JoinGroupRequestProtocol> Protocols { get; set; }
    }
}
