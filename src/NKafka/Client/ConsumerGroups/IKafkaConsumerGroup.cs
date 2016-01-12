using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroups
{
    [PublicAPI]
    public interface IKafkaConsumerGroup
    {
        string GroupName { get; }        
    }
}
