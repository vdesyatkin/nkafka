using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public interface IKafkaConsumerTopic
    {
        [CanBeNull] KafkaMessagePackage Consume(int? maxMessageCount = null);        
        void EnqueueCommit(long packageNumber);
        KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }

    [PublicAPI]
    public interface IKafkaConsumerTopic<TKey, TData>
    {
        [CanBeNull] KafkaMessagePackage<TKey,TData> Consume(int? maxMessageCount = null);
        void EnqueueCommit(long packageNumber);
        KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }
}
