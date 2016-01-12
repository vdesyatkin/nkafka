using JetBrains.Annotations;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public interface IKafkaConsumerGroup
    {
        string GroupName { get; }        
    }
}
