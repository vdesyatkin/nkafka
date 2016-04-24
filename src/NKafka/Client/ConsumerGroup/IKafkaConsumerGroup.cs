using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public interface IKafkaConsumerGroup
    {
        string GroupName { get; }
        KafkaConsumerGroupInfo GetDiagnosticsInfo();
    }
}
