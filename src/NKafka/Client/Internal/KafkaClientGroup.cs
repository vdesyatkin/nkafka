namespace NKafka.Client.Internal
{
    internal sealed class KafkaClientGroup
    {
        public readonly string GroupName;

        public KafkaClientGroup(string groupName)
        {
            GroupName = groupName;
        }
    }
}
