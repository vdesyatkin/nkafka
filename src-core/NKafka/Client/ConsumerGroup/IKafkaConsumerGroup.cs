using JetBrains.Annotations;
using NKafka.Client.ConsumerGroup.Diagnostics;

namespace NKafka.Client.ConsumerGroup
{
    [PublicAPI]
    public interface IKafkaConsumerGroup
    {
        [NotNull] string GroupName { get; }
        KafkaConsumerGroupType GroupType { get; }

        bool IsReady { get; }
        [NotNull] KafkaConsumerGroupInfo GetDiagnosticsInfo();
    }
}