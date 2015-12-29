using JetBrains.Annotations;

namespace NKafka.Client.Consumer
{
    [PublicAPI]
    public interface IKafkaConsumerTopic
    {
        [CanBeNull] KafkaMessagePackage Consume();        
        void Commit(int packageNumber);
    }

    [PublicAPI]
    public interface IKafkaConsumerTopic<TKey, TData>
    {
        [CanBeNull] KafkaMessagePackage<TKey,TData> Consume();
        void Commit(int packageNumber);
    }
}
