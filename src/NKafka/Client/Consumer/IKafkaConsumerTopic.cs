using JetBrains.Annotations;
using NKafka.Client.Consumer.Diagnostics;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public interface IKafkaConsumerTopic
    {
        [CanBeNull] KafkaMessagePackage Consume();        
        void Commit(int packageNumber);
        KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }

    [PublicAPI]
    public interface IKafkaConsumerTopic<TKey, TData>
    {
        [CanBeNull] KafkaMessagePackage<TKey,TData> Consume();
        void Commit(int packageNumber);
        KafkaConsumerTopicInfo GetDiagnosticsInfo();
    }
}
