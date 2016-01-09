using JetBrains.Annotations;

namespace NKafka.Protocol.API.GroupCoordinator
{
    /// <summary>
    /// <para>
    /// The offsets for a given consumer group are maintained by a specific broker called the group coordinator.<br/>
    /// i.e., a consumer needs to issue its offset commit and fetch requests to this specific broker. It can discover the current coordinator by issuing a group coordinator request.
    /// </para>
    /// </summary>
    [PublicAPI]
    internal sealed class KafkaGroupCoordinatorRequest : IKafkaRequest
    {
        public readonly string GroupId;

        public KafkaGroupCoordinatorRequest(string groupId)
        {
            GroupId = groupId;
        }
    }
}
