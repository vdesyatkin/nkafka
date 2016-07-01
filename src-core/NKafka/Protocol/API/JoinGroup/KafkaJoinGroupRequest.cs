using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NKafka.Protocol.API.JoinGroup
{
    /// <summary>
    /// <para>
    /// The JoinGroup request contains an array with the group protocols that it supports along with member-specific metadata. <br/>
    /// This is basically used to ensure compatibility of group member metadata within the group. <br/>
    /// The coordinator chooses a protocol which is supported by all members of the group and returns it in the respective JoinGroup responses. <br/>
    /// If a member joins and doesn't support any of the protocols used by the rest of the group, then it will be rejected. <br/>
    /// This mechanism provides a way to update protocol metadata to a new format in a rolling upgrade scenario. <br/>
    /// The newer version will provide metadata for the new protocol and for the old protocol, and the coordinator will choose the old protocol until all members have been upgraded.
    /// </para>
    /// </summary>
    [PublicAPI]
    public sealed class KafkaJoinGroupRequest : IKafkaRequest
    {
        public readonly string GroupName;

        public readonly string MemberId;
        
        public readonly TimeSpan SessionTimeout;

        public readonly IReadOnlyList<KafkaJoinGroupRequestProtocol> Protocols;

        public KafkaJoinGroupRequest(string groupName, string memberId, TimeSpan sessionTimeout, IReadOnlyList<KafkaJoinGroupRequestProtocol> protocols)
        {
            GroupName = groupName;
            MemberId = memberId;
            SessionTimeout = sessionTimeout;
            Protocols = protocols;
        }
    }
}
