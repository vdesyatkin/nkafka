using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    public interface IKafkaConsumerTopic
    {
        [CanBeNull] KafkaMessagePackage Consume();        
        void Commit(int packageNumber);
    }

    public interface IKafkaConsumerTopic<TKey, TData>
    {
        [CanBeNull] KafkaMessagePackage<TKey,TData> Consume();
        void Commit(int packageNumber);
    }
}
